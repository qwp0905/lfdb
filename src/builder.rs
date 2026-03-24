use std::{path::Path, time::Duration};

use crate::{
  utils::{LogLevel, Logger, NoneLogger, ToArc},
  Engine, EngineConfig, Result,
};

pub struct EngineBuilder<T>(EngineConfig<T>)
where
  T: AsRef<Path>;

impl<T> EngineBuilder<T>
where
  T: AsRef<Path>,
{
  pub fn new(base_path: T) -> Self {
    Self(EngineConfig {
      base_path,
      wal_file_size: DEFAULT_WAL_FILE_SIZE,
      wal_segment_flush_count: DEFAULT_WAL_SEGMENT_FLUSH_COUNT,
      wal_segment_flush_delay: DEFAULT_WAL_SEGMENT_FLUSH_DELAY,
      checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
      group_commit_count: DEFAULT_GROUP_COMMIT_COUNT,
      gc_trigger_interval: DEFAULT_GC_TRIGGER_INTERVAL,
      gc_thread_count: DEFAULT_GC_THREAD_COUNT,
      buffer_pool_shard_count: DEFAULT_BUFFER_POOL_SHARD_COUNT,
      buffer_pool_memory_capacity: DEFAULT_BUFFER_POOL_MEMORY_CAPACITY,
      transaction_timeout: DEFAULT_TRANSACTION_TIMEOUT,
      log_level: DEFAULT_LOG_LEVEL,
      logger: DEFAULT_LOGGER.to_arc(),
    })
  }

  pub fn wal_file_size(mut self, size: usize) -> Self {
    self.0.wal_file_size = size;
    self
  }
  pub fn wal_segment_flush_delay(mut self, delay: Duration) -> Self {
    self.0.wal_segment_flush_delay = delay;
    self
  }
  pub fn wal_segment_flush_count(mut self, count: usize) -> Self {
    self.0.wal_segment_flush_count = count;
    self
  }
  pub fn checkpoint_interval(mut self, interval: Duration) -> Self {
    self.0.checkpoint_interval = interval;
    self
  }
  pub fn group_commit_count(mut self, count: usize) -> Self {
    self.0.group_commit_count = count;
    self
  }
  pub fn buffer_pool_shard_count(mut self, count: usize) -> Self {
    self.0.buffer_pool_shard_count = count;
    self
  }
  pub fn buffer_pool_memory_capacity(mut self, capacity: usize) -> Self {
    self.0.buffer_pool_memory_capacity = capacity;
    self
  }
  pub fn gc_trigger_interval(mut self, interval: Duration) -> Self {
    self.0.gc_trigger_interval = interval;
    self
  }
  pub fn gc_thread_count(mut self, count: usize) -> Self {
    self.0.gc_thread_count = count;
    self
  }
  pub fn transaction_timeout(mut self, timeout: Duration) -> Self {
    self.0.transaction_timeout = timeout;
    self
  }
  pub fn logger<L: Logger + 'static>(mut self, logger: L) -> Self {
    self.0.logger = logger.to_arc();
    self
  }
  pub fn log_level(mut self, level: LogLevel) -> Self {
    self.0.log_level = level;
    self
  }

  pub fn build(self) -> Result<Engine> {
    Engine::bootstrap(self.0)
  }
}

const DEFAULT_WAL_FILE_SIZE: usize = 8 << 20; // 8 mb
const DEFAULT_WAL_SEGMENT_FLUSH_DELAY: Duration = Duration::from_secs(10);
const DEFAULT_WAL_SEGMENT_FLUSH_COUNT: usize = 32;
const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_GROUP_COMMIT_COUNT: usize = 512;
const DEFAULT_GC_TRIGGER_INTERVAL: Duration = Duration::from_secs(300);
const DEFAULT_GC_THREAD_COUNT: usize = 3;
const DEFAULT_BUFFER_POOL_SHARD_COUNT: usize = 1 << 6; // 64
const DEFAULT_BUFFER_POOL_MEMORY_CAPACITY: usize = 32 << 20; // 32 mb
const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_mins(3);
const DEFAULT_LOGGER: NoneLogger = NoneLogger;
const DEFAULT_LOG_LEVEL: LogLevel = LogLevel::Info;
