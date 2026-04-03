use std::{
  fs,
  panic::{RefUnwindSafe, UnwindSafe},
  path::Path,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::Duration,
};

use super::constant::{DATA_PATH, FILE_SUFFIX};
use crate::{
  buffer_pool::BufferPoolConfig,
  cursor::{GarbageCollectionConfig, TreeManagerConfig},
  disk::PAGE_SIZE,
  error::{Error, Result},
  metrics::{EngineMetrics, MetricsRegistry},
  transaction::{Transaction, TransactionConfig, TxOrchestrator},
  utils::{LogFilter, LogLevel, Logger, ToArc},
  wal::WALConfig,
};

pub struct EngineConfig<T>
where
  T: AsRef<Path>,
{
  pub base_path: T,
  pub wal_file_size: usize,
  pub wal_segment_flush_delay: Duration,
  pub wal_segment_flush_count: usize,
  pub checkpoint_interval: Duration,
  pub group_commit_count: usize,
  pub gc_trigger_interval: Duration,
  pub gc_thread_count: usize,
  pub buffer_pool_shard_count: usize,
  pub buffer_pool_memory_capacity: usize,
  pub transaction_timeout: Duration,
  pub logger: Arc<dyn Logger>,
  pub log_level: LogLevel,
}

pub struct Engine {
  orchestrator: Arc<TxOrchestrator>,
  available: AtomicBool,
  metrics_registry: Arc<MetricsRegistry>,
  logger: LogFilter,
}
impl Engine {
  pub fn bootstrap<T>(config: EngineConfig<T>) -> Result<Self>
  where
    T: AsRef<Path>,
  {
    let metrics_registry = MetricsRegistry::new().to_arc();
    let logger = LogFilter::new(config.log_level, config.logger);
    logger.info(|| "start engine");

    fs::create_dir_all(config.base_path.as_ref()).map_err(Error::IO)?;
    let wal_config = WALConfig {
      prefix: "wal".into(),
      checkpoint_interval: config.checkpoint_interval,
      group_commit_count: config.group_commit_count,
      max_file_size: config.wal_file_size,
      base_dir: config.base_path.as_ref().into(),
      segment_flush_count: config.wal_segment_flush_count,
      segment_flush_delay: config.wal_segment_flush_delay,
    };
    let buffer_pool_config = BufferPoolConfig {
      shard_count: config.buffer_pool_shard_count,
      capacity: config.buffer_pool_memory_capacity / PAGE_SIZE,
      path: config
        .base_path
        .as_ref()
        .join(format!("{}{}", DATA_PATH, FILE_SUFFIX)),
    };
    let gc_config = GarbageCollectionConfig {
      thread_count: config.gc_thread_count,
    };
    let tree_config = TreeManagerConfig {
      merge_interval: config.gc_trigger_interval,
    };
    let tx_config = TransactionConfig {
      timeout: config.transaction_timeout,
    };
    let orchestrator = TxOrchestrator::new(
      tx_config,
      buffer_pool_config,
      wal_config,
      gc_config,
      tree_config,
      logger.clone(),
      metrics_registry.clone(),
    )?;

    logger.info(|| "engine bootstrapped.");
    Ok(Self {
      orchestrator: orchestrator.to_arc(),
      available: AtomicBool::new(true),
      logger,
      metrics_registry,
    })
  }

  /**
   * create tranaction cursor with default timeout.
   */
  pub fn new_tx(&self) -> Result<Transaction<'_>> {
    if !self.available.load(Ordering::Acquire) {
      return Err(Error::EngineUnavailable);
    }
    let (state, snapshot) = self.orchestrator.start_tx(None)?;
    Ok(Transaction::new(
      self.orchestrator.clone(),
      state,
      snapshot,
      self.metrics_registry.clone(),
    ))
  }

  /**
   * create tranaction cursor with specified timeout.
   */
  pub fn new_tx_timeout(&self, timeout: Duration) -> Result<Transaction<'_>> {
    if !self.available.load(Ordering::Acquire) {
      return Err(Error::EngineUnavailable);
    }
    let (state, snapshot) = self.orchestrator.start_tx(Some(timeout))?;
    Ok(Transaction::new(
      self.orchestrator.clone(),
      state,
      snapshot,
      self.metrics_registry.clone(),
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
