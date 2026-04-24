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
      io_thread_count: DEFAULT_IO_THREAD_COUNT,
      wal_file_size: DEFAULT_WAL_FILE_SIZE,
      wal_segment_flush_count: DEFAULT_WAL_SEGMENT_FLUSH_COUNT,
      wal_segment_flush_delay: DEFAULT_WAL_SEGMENT_FLUSH_DELAY,
      checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
      group_commit_count: DEFAULT_GROUP_COMMIT_COUNT,
      gc_trigger_interval: DEFAULT_GC_TRIGGER_INTERVAL,
      gc_thread_count: DEFAULT_GC_THREAD_COUNT,
      compaction_threshold: DEFAULT_COMPACTION_THRESHOLD,
      block_cache_shard_count: DEFAULT_BLOCK_CACHE_SHARD_COUNT,
      block_cache_memory_capacity: DEFAULT_BLOCK_CACHE_MEMORY_CAPACITY,
      transaction_timeout: DEFAULT_TRANSACTION_TIMEOUT,
      log_level: DEFAULT_LOG_LEVEL,
      logger: DEFAULT_LOGGER.to_arc(),
    })
  }

  /**
   * Number of background IO worker threads shared across tables for write batching.
   * Each table holds at most one worker at a time.
   */
  pub fn io_thread_count(mut self, count: usize) -> Self {
    self.0.io_thread_count = count;
    self
  }

  /**
   * Size limit of a single WAL segment file. When exceeded, a new segment is created.
   * Larger segments improve write throughput by reducing rotation/checkpoint frequency,
   * but extend recovery time on crash since more records must be replayed before the engine becomes available.
   */
  pub fn wal_file_size(mut self, size: usize) -> Self {
    self.0.wal_file_size = size;
    self
  }
  /**
   * WAL segment reuse requires a checkpoint to confirm durability.
   * A checkpoint fires when either the commit count or the delay is reached,
   * after which the segment is reused.
   * Maximum time to wait before triggering a checkpoint for segment reuse.
   */
  pub fn wal_segment_flush_delay(mut self, delay: Duration) -> Self {
    self.0.wal_segment_flush_delay = delay;
    self
  }
  /**
   * WAL segment reuse requires a checkpoint to confirm durability.
   * A checkpoint fires when either the commit count or the delay is reached,
   * after which the segment is reused.
   * Maximum number of commits to buffer before triggering a checkpoint for segment reuse.
   */
  pub fn wal_segment_flush_count(mut self, count: usize) -> Self {
    self.0.wal_segment_flush_count = count;
    self
  }
  /**
   * Hard timeout for checkpoint execution. A checkpoint runs at this interval
   * regardless of WAL segment pressure, ensuring GC and durability are not
   * indefinitely deferred during idle periods.
   */
  pub fn checkpoint_interval(mut self, interval: Duration) -> Self {
    self.0.checkpoint_interval = interval;
    self
  }
  /**
   * Maximum commits buffered per WAL segment before flushing. A larger value
   * improves write throughput but increases potential data loss on crash in
   * high-latency IO environments.
   */
  pub fn group_commit_count(mut self, count: usize) -> Self {
    self.0.group_commit_count = count;
    self
  }
  /**
   * Number of block cache shards. More shards reduce lock contention by
   * narrowing each shard's scope, but too many shards shrink each shard's
   * capacity and increase eviction frequency, hurting performance.
   */
  pub fn block_cache_shard_count(mut self, count: usize) -> Self {
    self.0.block_cache_shard_count = count;
    self
  }
  /**
   * Total memory in bytes allocated to the block cache. Since the engine uses
   * direct IO and bypasses the OS page cache, a larger block cache is critical
   * for performance.
   */
  pub fn block_cache_memory_capacity(mut self, capacity: usize) -> Self {
    self.0.block_cache_memory_capacity = capacity;
    self
  }
  /**
   * Interval at which leaf merge runs. Run more frequently when removes are
   * heavy, less frequently when removes are rare, to maintain scan performance.
   */
  pub fn gc_trigger_interval(mut self, interval: Duration) -> Self {
    self.0.gc_trigger_interval = interval;
    self
  }
  /**
   * Number of threads used for GC. More threads speed up GC and therefore
   * checkpoint completion. In write-heavy workloads with frequent WAL segment
   * rotation, increasing this can improve write throughput.
   */
  pub fn gc_thread_count(mut self, count: usize) -> Self {
    self.0.gc_thread_count = count;
    self
  }
  /**
   * Maximum lifetime of a transaction before it is automatically aborted.
   */
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

  /**
   * Threshold which trigger auto compaction. To disable auto compaction, then set 1.0.
   */
  pub fn compaction_threshold(mut self, threshold: f64) -> Self {
    assert!(threshold <= 1.0);
    self.0.compaction_threshold = threshold;
    self
  }

  pub fn build(self) -> Result<Engine> {
    Engine::bootstrap(self.0)
  }
}

const DEFAULT_WAL_FILE_SIZE: usize = 512 << 20; // 512 mb
const DEFAULT_WAL_SEGMENT_FLUSH_DELAY: Duration = Duration::from_secs(10);
const DEFAULT_WAL_SEGMENT_FLUSH_COUNT: usize = 32;
const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_GROUP_COMMIT_COUNT: usize = 512;
const DEFAULT_GC_TRIGGER_INTERVAL: Duration = Duration::from_secs(300);
const DEFAULT_GC_THREAD_COUNT: usize = 5;
const DEFAULT_BLOCK_CACHE_SHARD_COUNT: usize = 1 << 6; // 64
const DEFAULT_BLOCK_CACHE_MEMORY_CAPACITY: usize = 32 << 20; // 32 mb
const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_mins(3);
const DEFAULT_LOGGER: NoneLogger = NoneLogger;
const DEFAULT_LOG_LEVEL: LogLevel = LogLevel::Info;
const DEFAULT_IO_THREAD_COUNT: usize = 32;
const DEFAULT_COMPACTION_THRESHOLD: f64 = 0.5;
