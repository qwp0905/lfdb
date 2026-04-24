use std::{
  collections::HashMap,
  fs,
  panic::{RefUnwindSafe, UnwindSafe},
  path::Path,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use crate::{
  cache::{BlockCache, BlockCacheConfig},
  cursor::{GarbageCollectionConfig, GarbageCollector, TreeManager, TreeManagerConfig},
  disk::PAGE_SIZE,
  error::{Error, Result},
  metrics::{EngineMetrics, MetricsRegistry},
  table::{TableConfig, TableMapper, META_TABLE_ID},
  transaction::{
    PageRecorder, Transaction, TransactionConfig, TxOrchestrator, VersionVisibility,
  },
  utils::{LogFilter, LogLevel, Logger, ToArc},
  wal::{WALConfig, WAL},
};

pub struct EngineConfig<T>
where
  T: AsRef<Path>,
{
  pub base_path: T,
  pub io_thread_count: usize,
  pub wal_file_size: usize,
  pub wal_segment_flush_delay: Duration,
  pub wal_segment_flush_count: usize,
  pub checkpoint_interval: Duration,
  pub group_commit_count: usize,
  pub gc_trigger_interval: Duration,
  pub gc_thread_count: usize,
  pub compaction_threshold: f64,
  pub block_cache_shard_count: usize,
  pub block_cache_memory_capacity: usize,
  pub transaction_timeout: Duration,
  pub logger: Arc<dyn Logger>,
  pub log_level: LogLevel,
}

pub struct Engine {
  orchestrator: TxOrchestrator,
  available: AtomicBool,
  metrics_registry: Arc<MetricsRegistry>,
  logger: LogFilter,
}
impl Engine {
  pub fn bootstrap<T>(config: EngineConfig<T>) -> Result<Self>
  where
    T: AsRef<Path>,
  {
    let st = Instant::now();
    let metrics_registry = MetricsRegistry::new().to_arc();
    let logger = LogFilter::new(config.log_level, config.logger);
    logger.info(|| "start engine");

    fs::create_dir_all(config.base_path.as_ref()).map_err(Error::IO)?;
    let wal_config = WALConfig {
      group_commit_count: config.group_commit_count,
      max_file_size: config.wal_file_size,
      base_dir: config.base_path.as_ref().into(),
      segment_flush_count: config.wal_segment_flush_count,
      segment_flush_delay: config.wal_segment_flush_delay,
    };
    let block_cache_config = BlockCacheConfig {
      shard_count: config.block_cache_shard_count,
      capacity: config.block_cache_memory_capacity / PAGE_SIZE,
    };
    let gc_config = GarbageCollectionConfig {
      thread_count: config.gc_thread_count,
    };
    let tree_config = TreeManagerConfig {
      merge_interval: config.gc_trigger_interval,
      compaction_threshold: config.compaction_threshold,
    };
    let tx_config = TransactionConfig {
      timeout: config.transaction_timeout,
      checkpoint_interval: config.checkpoint_interval,
    };
    let table_config = TableConfig {
      base_path: config.base_path.as_ref().into(),
      io_thread_count: config.io_thread_count,
    };

    let block_cache =
      BlockCache::open(block_cache_config, logger.clone(), metrics_registry.clone())?
        .to_arc();
    let tables = TableMapper::new(table_config, metrics_registry.clone())?.to_arc();

    let (wal, replay) = WAL::replay(&wal_config, logger.clone())?;
    let wal = wal.to_arc();

    let recorder = PageRecorder::new(wal.clone()).to_arc();
    let version_visibility =
      VersionVisibility::new(replay.aborted, replay.last_tx_id).to_arc();

    let gc = GarbageCollector::start(
      block_cache.clone(),
      version_visibility.clone(),
      recorder.clone(),
      tables.clone(),
      logger.clone(),
      gc_config,
    )
    .to_arc();

    if tables.is_new() {
      logger.info(|| "engine initial state.");
      let tree_manager = TreeManager::initialize(
        block_cache.clone(),
        tables.clone(),
        recorder.clone(),
        gc.clone(),
        wal.clone(),
        version_visibility.clone(),
        logger.clone(),
        tree_config,
      )?;
      let orchestrator = TxOrchestrator::new(
        tx_config,
        &wal_config,
        wal,
        block_cache,
        tables,
        version_visibility,
        gc,
        recorder,
        logger.clone(),
        tree_manager,
        metrics_registry.clone(),
      );

      logger.info(|| format!("engine bootstrapped in {} secs.", st.elapsed().as_secs()));
      return Ok(Self {
        orchestrator,
        available: AtomicBool::new(true),
        metrics_registry,
        logger,
      });
    }

    logger.info(|| "trying to replay...");

    // To recover table information, first replay the metadata table
    let meta_table = tables.meta_table();
    for (_, ptr, data) in replay
      .redo
      .iter()
      .filter(|(table_id, _, _)| *table_id == META_TABLE_ID)
    {
      block_cache
        .read(*ptr, meta_table.clone())?
        .for_write()
        .as_mut()
        .writer()
        .write(data)?;
    }

    let mut handles = HashMap::new();
    let (open_handles, compactions) =
      TreeManager::open_handles(&block_cache, &version_visibility, &tables)?;
    for table in open_handles {
      handles.insert(table.metadata().get_id(), table);
    }
    for (table, c_table) in compactions.iter() {
      handles.insert(table.metadata().get_id(), table.handle().clone());
      handles.insert(c_table.metadata().get_id(), c_table.handle().clone());
    }

    for (table_id, ptr, data) in replay
      .redo
      .iter()
      .filter(|(table_id, _, _)| *table_id != META_TABLE_ID)
    {
      let handle = match handles.get(table_id) {
        Some(handle) => handle.clone(),
        None => continue,
      };
      block_cache
        .read(*ptr, handle)?
        .for_write()
        .as_mut()
        .writer()
        .write(data)?;
    }

    // Flush replayed pages to disk so disk_len reflects the true file extent.
    // TreeManager::clean_and_start uses disk_len to scan for orphaned blocks
    // (allocated but unreferenced pages) and reclaim them into the free list.
    block_cache.flush()?;
    tables.replay(handles.into_values())?;

    let tree_manager = TreeManager::clean_and_start(
      block_cache.clone(),
      tables.clone(),
      recorder.clone(),
      gc.clone(),
      wal.clone(),
      version_visibility.clone(),
      logger.clone(),
      tree_config,
    )?;

    for (table, c_table) in compactions {
      tree_manager.resume_compact(table, c_table);
    }

    let orchestrator = TxOrchestrator::initial_checkpoint(
      tx_config,
      &wal_config,
      wal,
      block_cache,
      tables,
      version_visibility,
      gc,
      recorder,
      logger.clone(),
      tree_manager,
      metrics_registry.clone(),
      replay.segments,
    )?;

    logger.info(|| format!("engine bootstrapped in {} secs.", st.elapsed().as_secs()));
    Ok(Self {
      orchestrator,
      available: AtomicBool::new(true),
      logger,
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
    let (state, snapshot) = self.orchestrator.start_tx(None)?;
    Ok(Transaction::new(
      &self.orchestrator,
      state,
      snapshot,
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
    let (state, snapshot) = self.orchestrator.start_tx(Some(timeout))?;
    Ok(Transaction::new(
      &self.orchestrator,
      state,
      snapshot,
      &self.metrics_registry,
    ))
  }

  pub fn metrics(&self) -> EngineMetrics {
    self.metrics_registry.snapshot()
  }
}

impl Drop for Engine {
  fn drop(&mut self) {
    if let Ok(_) =
      self
        .available
        .compare_exchange(true, false, Ordering::Release, Ordering::Acquire)
    {
      self.logger.info(|| "engine shutdown");
      if let Err(err) = self.orchestrator.close() {
        self.logger.error(|| err.to_string());
      };
    }
  }
}

unsafe impl Send for Engine {}
unsafe impl Sync for Engine {}
impl UnwindSafe for Engine {}
impl RefUnwindSafe for Engine {}
