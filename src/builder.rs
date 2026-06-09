use std::{path::Path, time::Duration};

use crate::{Engine, EngineConfig, Result};

pub struct EngineBuilder<T>(EngineConfig<T>)
where
  T: AsRef<Path>;

impl<T> EngineBuilder<T>
where
  T: AsRef<Path>,
{
  pub const fn new(base_path: T) -> Self {
    Self(EngineConfig {
      base_path,
      io_thread_count: DEFAULT_IO_THREAD_COUNT,
      wal_file_size: DEFAULT_WAL_FILE_SIZE,
      wal_buffer_size: DEFAULT_WAL_BUFFER_SIZE,
      checkpoint_flush_factor: DEFAULT_FLUSH_FACTOR,
      gc_trigger_interval: DEFAULT_GC_TRIGGER_INTERVAL,
      gc_thread_count: DEFAULT_GC_THREAD_COUNT,
      gc_batch_size: DEFAULT_GC_BATCH_SIZE,
      compaction_threshold: DEFAULT_COMPACTION_THRESHOLD,
      compaction_min_size: DEFAULT_COMPACTION_MIN_SIZE,
      compaction_batch_size: DEFAULT_COMPACTION_BATCH_SIZE,
      block_cache_shard_count: DEFAULT_BLOCK_CACHE_SHARD_COUNT,
      block_cache_memory_capacity: DEFAULT_BLOCK_CACHE_MEMORY_CAPACITY,
      transaction_timeout: DEFAULT_TRANSACTION_TIMEOUT,
    })
  }

  /**
   * Number of background IO worker threads shared across tables for write batching.
   * Each table holds at most one worker at a time.
   */
  pub const fn io_thread_count(mut self, count: usize) -> Self {
    self.0.io_thread_count = count;
    self
  }

  /**
   * Size limit of a single WAL segment file. When exceeded, a new segment is created.
   * Larger segments improve write throughput by reducing rotation/checkpoint frequency,
   * but extend recovery time on crash since more records must be replayed before the engine becomes available.
   */
  pub const fn wal_file_size(mut self, size: usize) -> Self {
    self.0.wal_file_size = size;
    self
  }
  /**
   * Soft limit of WAL buffer size.
   */
  pub const fn wal_buffer_size(mut self, size: usize) -> Self {
    self.0.wal_buffer_size = size;
    self
  }
  /**
   * Determines the growth factor at which to flush the block cache at the checkpoint.
   * In environments with frequent WAL segment replacement,
   * such as write-heavy workloads, pressure increases exponentially at the set ratio.
   *
   */
  pub const fn checkpoint_flush_factor(mut self, factor: f64) -> Self {
    assert!(
      factor.is_finite(),
      "checkpoint flush pressure must be finite"
    );
    assert!(
      factor >= 1.0,
      "checkpoint flush pressure must be gte then 1.0"
    );
    self.0.checkpoint_flush_factor = factor;
    self
  }
  /**
   * Number of block cache shards. More shards reduce lock contention by
   * narrowing each shard's scope, but too many shards shrink each shard's
   * capacity and increase eviction frequency, hurting performance.
   */
  pub const fn block_cache_shard_count(mut self, count: usize) -> Self {
    self.0.block_cache_shard_count = count;
    self
  }
  /**
   * Total memory in bytes allocated to the block cache. Since the engine uses
   * direct IO and bypasses the OS page cache, a larger block cache is critical
   * for performance.
   */
  pub const fn block_cache_memory_capacity(mut self, capacity: usize) -> Self {
    self.0.block_cache_memory_capacity = capacity;
    self
  }
  /**
   * Interval at which garbage collection runs. Run more frequently when removes are
   * heavy, less frequently when removes are rare, to maintain scan performance.
   */
  pub const fn gc_trigger_interval(mut self, interval: Duration) -> Self {
    self.0.gc_trigger_interval = interval;
    self
  }
  /**
   * Number of keys to advance per gc tick.
   */
  pub const fn gc_batch_size(mut self, count: usize) -> Self {
    self.0.gc_batch_size = count;
    self
  }
  /**
   * Number of threads used for GC. More threads speed up GC and therefore
   * checkpoint completion. In write-heavy workloads with frequent WAL segment
   * rotation, increasing this can improve write throughput.
   */
  pub const fn gc_thread_count(mut self, count: usize) -> Self {
    self.0.gc_thread_count = count;
    self
  }
  /**
   * Maximum lifetime of a transaction before it is automatically aborted.
   */
  pub const fn transaction_timeout(mut self, timeout: Duration) -> Self {
    self.0.transaction_timeout = timeout;
    self
  }

  /**
   * Threshold which trigger auto compaction. To disable auto compaction, then set 1.0.
   */
  pub const fn compaction_threshold(mut self, threshold: f64) -> Self {
    assert!(threshold <= 1.0);
    self.0.compaction_threshold = threshold;
    self
  }

  /**
   * Minimum size requirements for auto compaction triggers.
   */
  pub const fn compaction_min_size(mut self, size: usize) -> Self {
    self.0.compaction_min_size = size;
    self
  }

  /**
   * Number of keys to copy per compaction tick.
   */
  pub const fn compaction_batch_size(mut self, size: usize) -> Self {
    self.0.compaction_batch_size = size;
    self
  }

  pub fn build(&self) -> Result<Engine> {
    Engine::bootstrap(&self.0)
  }
}

const DEFAULT_WAL_FILE_SIZE: usize = 128 << 20; // 64 mb
const DEFAULT_WAL_BUFFER_SIZE: usize = 8 << 20;
const DEFAULT_FLUSH_FACTOR: f64 = 1.25;
const DEFAULT_GC_TRIGGER_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_GC_BATCH_SIZE: usize = 32;
const DEFAULT_GC_THREAD_COUNT: usize = 3;
const DEFAULT_BLOCK_CACHE_SHARD_COUNT: usize = 1 << 6; // 64
const DEFAULT_BLOCK_CACHE_MEMORY_CAPACITY: usize = 32 << 20; // 32 mb
const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_mins(3);
const DEFAULT_IO_THREAD_COUNT: usize = 32;
const DEFAULT_COMPACTION_THRESHOLD: f64 = 0.5;
const DEFAULT_COMPACTION_MIN_SIZE: usize = 512 << 20;
const DEFAULT_COMPACTION_BATCH_SIZE: usize = 128;
