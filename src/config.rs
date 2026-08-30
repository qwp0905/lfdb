use super::{Error, Result};
use std::{path::PathBuf, time::Duration};

macro_rules! invalid {
  ($msg:tt) => {
    Err(Error::InvalidConfig($msg))
  };
}

pub struct EngineConfig {
  pub base_path: PathBuf,
  pub io_thread_count: usize,
  pub wal_file_size: usize,
  pub wal_buffer_size: usize,
  pub checkpoint_flush_factor: f64,
  pub gc_batch_size: usize,
  pub compaction_threshold: f64,
  pub compaction_min_size: usize,
  pub compaction_batch_size: usize,
  pub block_cache_shard_count: usize,
  pub block_cache_memory_capacity: usize,
  pub transaction_timeout: Duration,
}
impl EngineConfig {
  pub fn validate(&self) -> Result {
    if self.io_thread_count == 0 {
      return invalid!("io_thread_count must be greater than 0.");
    }

    if self.wal_file_size == 0 {
      return invalid!("wal_file_size must be greater than 0.");
    }

    if self.wal_buffer_size == 0 {
      return invalid!("wal_buffer_size must be greater than 0.");
    }

    if !self.checkpoint_flush_factor.is_finite() {
      return invalid!("checkpoint_flush_factor must be finite");
    }

    if self.checkpoint_flush_factor < 1.0 {
      return invalid!("checkpoint_flush_factor must be equal or greater than 1.0");
    }

    if self.block_cache_shard_count == 0 {
      return invalid!("block_cache_shard_count must be greater than 0.");
    }

    if self.block_cache_memory_capacity == 0 {
      return invalid!("block_cache_memory_capacity must be greater than 0.");
    }

    if self.gc_batch_size == 0 {
      return invalid!("gc_batch_size must be greater than 0.");
    }

    if self.transaction_timeout == Duration::ZERO {
      return invalid!("transaction_timeout must be greater than 0.");
    }

    if self.compaction_threshold > 1.0 {
      return invalid!("compaction_threshold must be equal or less than 1.0");
    }

    if self.compaction_threshold <= 0.0 {
      return invalid!("compaction_threshold must be greater than 0.0");
    }

    if self.compaction_min_size == 0 {
      return invalid!("compaction_min_size must be greater than 0.");
    }

    if self.compaction_batch_size == 0 {
      return invalid!("compaction_batch_size must be greater than 0.");
    }

    Ok(())
  }
}
impl Clone for EngineConfig {
  fn clone(&self) -> Self {
    Self {
      base_path: self.base_path.clone(),
      io_thread_count: self.io_thread_count,
      wal_file_size: self.wal_file_size,
      wal_buffer_size: self.wal_buffer_size,
      checkpoint_flush_factor: self.checkpoint_flush_factor,
      gc_batch_size: self.gc_batch_size,
      compaction_threshold: self.compaction_threshold,
      compaction_min_size: self.compaction_min_size,
      compaction_batch_size: self.compaction_batch_size,
      block_cache_shard_count: self.block_cache_shard_count,
      block_cache_memory_capacity: self.block_cache_memory_capacity,
      transaction_timeout: self.transaction_timeout,
    }
  }
}
