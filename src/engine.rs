use std::{
  collections::HashMap,
  panic::{RefUnwindSafe, UnwindSafe},
  path::Path,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use super::EngineConfig;
use crate::{
  background::EventBus,
  cache::{BlockCache, BlockCacheConfig},
  cursor::{
    initialize, open_tables, CompactionConfig, CompactionPublished, Compactor,
    GarbageCollectionConfig, GarbageCollector,
  },
  disk::{DiskBackend, IOPool, Pointer, PAGE_SIZE},
  error, info,
  metrics::{EngineMetrics, MetricsRegistry},
  table::{TableMapper, META_TABLE_ID},
  transaction::{
    Checkpoint, PageRecorder, Transaction, TransactionConfig, TxOrchestrator,
    VersionVisibility,
  },
  utils::ToArc,
  wal::{WALConfig, WAL},
  Error, Result,
};

pub struct Engine {
  orchestrator: TxOrchestrator,
  event_bus: Arc<EventBus>,
  available: AtomicBool,
  metrics_registry: Arc<MetricsRegistry>,
}
impl Engine {
  pub fn bootstrap<T, B>(backend: B, config: &EngineConfig<T>) -> Result<Self>
  where
    T: AsRef<Path>,
    B: DiskBackend,
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
    };
    let gc_config = GarbageCollectionConfig {
      thread_count: config.gc_thread_count,
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
    let tables = TableMapper::new(io_pool.clone())?.to_arc();

    let (wal, replay) = WAL::replay(&wal_config, event_bus.clone(), io_pool.clone())?;
    let wal = wal.to_arc();

    let recorder = PageRecorder::new(wal.clone()).to_arc();
    let version_visibility = VersionVisibility::replay(
      io_pool.clone(),
      replay.last_tx_id,
      replay.started,
      replay.closed,
      replay.last_snapshot,
      &event_bus,
    )?;

    if tables.is_new() {
      info!("engine initial state.");
      initialize(&block_cache, &tables, &recorder, &version_visibility)?;

      let gc = GarbageCollector::new(
        block_cache.clone(),
        version_visibility.clone(),
        recorder.clone(),
        tables.clone(),
        event_bus.clone(),
        gc_config,
      );

      let compactor = Compactor::new(
        block_cache.clone(),
        tables.clone(),
        recorder.clone(),
        version_visibility.clone(),
        wal.clone(),
        event_bus.clone(),
        compaction_config,
      );

      let checkpoint = Checkpoint::new(
        wal.clone(),
        block_cache.clone(),
        version_visibility.clone(),
        io_pool.clone(),
        event_bus.clone(),
        metrics_registry.clone(),
        config.checkpoint_flush_factor,
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
    }

    info!("trying to replay...");

    // To recover table information, first replay the metadata table
    let meta_table = tables.meta_table();
    for (_, ptr, data) in replay
      .redo
      .iter()
      .filter(|(table_id, _, _)| *table_id == META_TABLE_ID)
    {
      block_cache
        .read_unchecked(*ptr, &meta_table)?
        .for_write()
        .as_mut()
        .writer()
        .write(data)?;
    }

    let mut handles = HashMap::new();
    let (open_handles, compactions) =
      open_tables(&block_cache, &tables, &version_visibility)?;
    for (table, metadata) in open_handles {
      handles.insert(table.get_id(), (metadata, table));
    }
    for ((table, metadata), (c_table, c_meta)) in compactions.iter() {
      handles.insert(table.get_id(), (metadata.clone(), table.handle().clone()));
      handles.insert(c_table.get_id(), (c_meta.clone(), c_table.handle().clone()));
    }

    for (table_id, ptr, data) in replay
      .redo
      .iter()
      .filter(|(table_id, _, _)| *table_id != META_TABLE_ID)
    {
      let Some((_, handle)) = handles.get(table_id) else {
        continue;
      };
      block_cache
        .read_unchecked(*ptr, handle)?
        .for_write()
        .as_mut()
        .writer()
        .write(data)?;
    }

    block_cache.create_flusher().flush_hard()?;
    tables.replay(handles.into_values())?;

    let gc = GarbageCollector::new(
      block_cache.clone(),
      version_visibility.clone(),
      recorder.clone(),
      tables.clone(),
      event_bus.clone(),
      gc_config,
    );

    let compactor = Compactor::new(
      block_cache.clone(),
      tables.clone(),
      recorder.clone(),
      version_visibility.clone(),
      wal.clone(),
      event_bus.clone(),
      compaction_config,
    );

    let events = compactions
      .into_iter()
      .map(|((table, _), (c_table, c_meta))| {
        CompactionPublished::new(table, c_table, c_meta)
      });
    event_bus.batch_publish(events);

    let checkpoint = Checkpoint::initial_checkpoint(
      wal.clone(),
      block_cache.clone(),
      version_visibility.clone(),
      io_pool.clone(),
      event_bus.clone(),
      metrics_registry.clone(),
      config.checkpoint_flush_factor,
    )?;
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
