use std::{
  collections::HashMap,
  panic::{RefUnwindSafe, UnwindSafe},
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use super::EngineConfig;
use crate::{
  background::EventBus,
  blob::BlobStorage,
  cache::{BlockCache, BlockCacheConfig},
  cursor::{
    initialize, open_tables, recovery, CompactionConfig, CompactionPublished,
    CompactionTriggered, Compactor, GarbageCollectionConfig, GarbageCollector,
  },
  disk::{DiskBackend, IOPool, Pointer, PAGE_SIZE},
  error, info,
  manifest::{load_manifest, save_manifest, Manifest},
  metrics::{EngineMetrics, MetricsRegistry},
  table::{TableFormatVersion, TableId, TableMapper},
  transaction::{
    Checkpoint, CheckpointSnapshot, PageRecorder, SnapshotFormatVersion, Transaction,
    TransactionConfig, TxOrchestrator, VersionVisibility,
  },
  utils::ToArc,
  wal::{WALConfig, WALFormatVersion, WriteAheadLog},
  Error, Result,
};

pub struct Engine {
  orchestrator: TxOrchestrator,
  event_bus: Arc<EventBus>,
  /**
   * Engine-level availability flag.
   *
   * `Engine` mainly exposes bootstrapping and transaction creation, so this flag
   * is observed at transaction creation time, but its meaning is broader: whether
   * this engine instance is still usable.
   */
  available: AtomicBool,
  metrics_registry: Arc<MetricsRegistry>,
}
impl Engine {
  pub fn bootstrap<B>(backend: B, config: &EngineConfig) -> Result<Self>
  where
    B: DiskBackend + 'static,
  {
    let st = Instant::now();
    config.validate()?;
    let metrics_registry = MetricsRegistry::new().to_arc();

    let event_bus = Arc::new(EventBus::new());

    info!("start engine");

    let io_pool = IOPool::with_backend(
      backend,
      config.io_thread_count,
      config.base_path.as_ref(),
      metrics_registry.clone(),
    )?
    .to_arc();

    let wal_config = WALConfig {
      max_file_size: config.wal_file_size,
      max_buffer_size: config.wal_buffer_size,
    };
    let block_cache_config = BlockCacheConfig {
      shard_count: config.block_cache_shard_count,
      capacity: config.block_cache_memory_capacity / PAGE_SIZE,
      buffer_size: config.block_cache_buffer_size / PAGE_SIZE,
    };
    let gc_config = GarbageCollectionConfig {
      batch_size: config.gc_batch_size,
      compact_threshold: config.compaction_threshold,
      compact_min_size: (config.compaction_min_size / PAGE_SIZE) as Pointer,
    };
    let compaction_config = CompactionConfig {
      batch_size: config.compaction_batch_size,
    };
    let tx_config = TransactionConfig {
      timeout: config.transaction_timeout,
      checkpoint_flush_factor: config.checkpoint_flush_factor,
    };

    let block_cache =
      BlockCache::open(block_cache_config, metrics_registry.clone())?.to_arc();

    let Some(mut manifest) = load_manifest(&io_pool)? else {
      info!("engine initial state.");
      let (tables, metadata) = TableMapper::open_new(io_pool.clone())?;

      let tables = tables.to_arc();
      let wal =
        WriteAheadLog::init(&wal_config, event_bus.clone(), io_pool.clone())?.to_arc();
      let blob = BlobStorage::init(io_pool.clone(), wal.clone()).to_arc();
      let recorder = PageRecorder::new(wal.clone()).to_arc();
      let version_visibility = VersionVisibility::init(&event_bus);

      initialize(&block_cache, &tables, &recorder, &version_visibility, &blob)?;

      let manifest = Manifest::new(
        PAGE_SIZE as u32,
        metadata,
        SnapshotFormatVersion::CURRENT,
        WALFormatVersion::CURRENT,
      );
      save_manifest(&io_pool, &manifest)?;
      io_pool.sync_dir()?;

      let checkpoint = Checkpoint::new(
        wal.clone(),
        block_cache.clone(),
        version_visibility.clone(),
        io_pool.clone(),
        blob.clone(),
        event_bus.clone(),
        metrics_registry.clone(),
        config.checkpoint_flush_factor,
      );

      let gc = GarbageCollector::new(
        block_cache.clone(),
        version_visibility.clone(),
        recorder.clone(),
        tables.clone(),
        event_bus.clone(),
        blob.clone(),
        gc_config,
      );

      let compactor = Compactor::new(
        block_cache.clone(),
        tables.clone(),
        recorder.clone(),
        version_visibility.clone(),
        wal.clone(),
        event_bus.clone(),
        blob.clone(),
        compaction_config,
      );

      let orchestrator = TxOrchestrator::new(
        tx_config,
        wal,
        block_cache,
        tables,
        version_visibility,
        gc,
        recorder,
        compactor,
        io_pool,
        blob,
        checkpoint,
        metrics_registry.clone(),
      );

      info!("engine bootstrapped in {} secs.", st.elapsed().as_secs());
      return Ok(Self {
        orchestrator,
        event_bus,
        available: AtomicBool::new(true),
        metrics_registry,
      });
    };

    if manifest.page_size != PAGE_SIZE as u32 {
      return Err(Error::UnsupportedPageSize);
    }

    let tables =
      TableMapper::open_exists(io_pool.clone(), &manifest.metadata_table)?.to_arc();

    info!("trying to replay...");
    let (wal, replay) = WriteAheadLog::replay(
      &wal_config,
      event_bus.clone(),
      io_pool.clone(),
      manifest.wal_version,
    )?;
    let wal = wal.to_arc();

    let snapshot = match replay.last_snapshot {
      Some(file) => {
        let mut handle = io_pool.open_scan_io(file)?;
        CheckpointSnapshot::read_from(&mut handle, manifest.snapshot_version)?
      }
      None => CheckpointSnapshot::empty(),
    };
    let mut blob_metadata = snapshot.blob_metadata;
    blob_metadata.extend(replay.blob_handles);
    let blob = BlobStorage::replay(blob_metadata, io_pool.clone(), wal.clone())?.to_arc();

    let recorder = PageRecorder::new(wal.clone()).to_arc();
    let version_visibility = VersionVisibility::replay(
      replay.last_tx_id,
      replay.started,
      replay.closed,
      snapshot.active_versions,
      snapshot.aborted_versions,
      &event_bus,
    )?;

    let mut max_used = HashMap::<TableId, Pointer>::new();
    // To recover table information, first replay the metadata table
    let meta_table = tables.meta_table();
    let meta_table_id = meta_table.get_id();
    for (_, ptr, data) in replay
      .redo
      .iter()
      .filter(|(table_id, _, _)| *table_id == meta_table_id)
    {
      max_used
        .entry(meta_table_id)
        .and_modify(|v| *v = (*v).max(*ptr))
        .or_insert(*ptr);
      block_cache
        .read_unchecked(*ptr, &meta_table)?
        .for_write()
        .as_mut()
        .writer()
        .write(data)?;
    }

    let mut handles = HashMap::new();
    let found_handles = open_tables(&block_cache, &tables, &version_visibility, &blob)?;
    for (table, metadata) in found_handles.handles.iter() {
      handles.insert(table.get_id(), (metadata.clone(), table.clone()));
    }
    for ((table, metadata), (c_table, c_meta)) in found_handles.in_compaction.iter() {
      handles.insert(table.get_id(), (metadata.clone(), table.clone()));
      handles.insert(c_table.get_id(), (c_meta.clone(), c_table.clone()));
    }

    for (table_id, ptr, data) in replay
      .redo
      .iter()
      .filter(|(table_id, _, _)| *table_id != meta_table_id)
    {
      let Some((_, handle)) = handles.get(table_id) else {
        continue;
      };
      max_used
        .entry(*table_id)
        .and_modify(|v| *v = (*v).max(*ptr))
        .or_insert(*ptr);
      block_cache
        .read_unchecked(*ptr, handle)?
        .for_write()
        .as_mut()
        .writer()
        .write(data)?;
    }

    let checkpoint = Checkpoint::initial_checkpoint(
      wal.clone(),
      block_cache.clone(),
      version_visibility.clone(),
      io_pool.clone(),
      blob.clone(),
      event_bus.clone(),
      metrics_registry.clone(),
      config.checkpoint_flush_factor,
    )?;

    tables.replay(handles.into_values())?;

    if !meta_table.get_version().is_current() {
      info!(
        "metadata table format version has been expired. it will be updated from {} to {}",
        meta_table.get_version(),
        TableFormatVersion::CURRENT
      );

      // TODO: Implement offline compaction of the metadata table here when a change to the table format version is actually required.
      // manifest.metadata_table.version = TableFormatVersion::CURRENT;
    }

    manifest.snapshot_version = SnapshotFormatVersion::CURRENT;
    manifest.wal_version = WALFormatVersion::CURRENT;
    save_manifest(&io_pool, &manifest)?;

    recovery(block_cache.clone(), recorder.clone(), &tables, max_used)?;

    let gc = GarbageCollector::new(
      block_cache.clone(),
      version_visibility.clone(),
      recorder.clone(),
      tables.clone(),
      event_bus.clone(),
      blob.clone(),
      gc_config,
    );

    let compactor = Compactor::new(
      block_cache.clone(),
      tables.clone(),
      recorder.clone(),
      version_visibility.clone(),
      wal.clone(),
      event_bus.clone(),
      blob.clone(),
      compaction_config,
    );

    let events = found_handles
      .handles
      .into_iter()
      .filter(|(_, metadata)| !metadata.get_version().is_current())
      .map(|(table, _)| CompactionTriggered::new(table));
    event_bus.batch_publish(events);

    let events =
      found_handles
        .in_compaction
        .into_iter()
        .map(|((table, _), (c_table, c_meta))| {
          CompactionPublished::new(table, c_table, c_meta)
        });
    event_bus.batch_publish(events);

    // Discard replay input WAL segments after the initial checkpoint.
    // Restart may use a different WAL segment size/configuration. Rather than
    // validating and resizing old segment files for reuse, boot removes the replayed
    // files and lets the new WAL/preload configuration create fresh segments.
    replay
      .segments
      .into_iter()
      .try_for_each(|seg| seg.truncate())?;

    let orchestrator = TxOrchestrator::new(
      tx_config,
      wal,
      block_cache,
      tables,
      version_visibility,
      gc,
      recorder,
      compactor,
      io_pool,
      blob,
      checkpoint.clone(),
      metrics_registry.clone(),
    );

    info!("engine bootstrapped in {} secs.", st.elapsed().as_secs());
    Ok(Self {
      orchestrator,
      event_bus,
      available: AtomicBool::new(true),
      metrics_registry,
    })
  }

  /**
   * create transaction cursor with default timeout.
   */
  pub fn new_tx(&self) -> Result<Transaction<'_>> {
    if !self.available.load(Ordering::Acquire) {
      return Err(Error::EngineUnavailable);
    }
    let Some((state, snapshot)) = self.orchestrator.start_tx(None) else {
      return Err(Error::EngineUnavailable);
    };
    Ok(Transaction::new(
      &self.orchestrator,
      state,
      snapshot,
      &self.event_bus,
      &self.metrics_registry,
    ))
  }

  /**
   * create transaction cursor with specified timeout.
   */
  pub fn new_tx_timeout(&self, timeout: Duration) -> Result<Transaction<'_>> {
    if !self.available.load(Ordering::Acquire) {
      return Err(Error::EngineUnavailable);
    }
    let Some((state, snapshot)) = self.orchestrator.start_tx(Some(timeout)) else {
      return Err(Error::EngineUnavailable);
    };
    Ok(Transaction::new(
      &self.orchestrator,
      state,
      snapshot,
      &self.event_bus,
      &self.metrics_registry,
    ))
  }

  pub fn metrics(&self) -> EngineMetrics {
    self.metrics_registry.snapshot()
  }
}

impl Drop for Engine {
  fn drop(&mut self) {
    if self
      .available
      .compare_exchange(true, false, Ordering::Release, Ordering::Acquire)
      .is_ok()
    {
      info!("engine shutdown");
      if let Err(err) = self.orchestrator.close() {
        error!("error occurs in close engine: {err}");
      };

      self.event_bus.close();
    }
  }
}

unsafe impl Send for Engine {}
unsafe impl Sync for Engine {}
impl UnwindSafe for Engine {}
impl RefUnwindSafe for Engine {}
