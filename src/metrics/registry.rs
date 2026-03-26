use std::time::Duration;

use super::{Counter, Histogram};

#[derive(Debug)]
pub struct EngineMetrics {
  pub buffer_pool_cache_hit: u64,
  pub buffer_pool_cache_miss: u64,
  pub buffer_pool_flush_count: u64,
  pub buffer_pool_flush_latency_ms_average: f64,

  pub transaction_start_count: u64,
  pub transaction_abort_count: u64,

  pub transaction_commit_count: u64,
  pub transaction_commit_latency_ms_average: f64,
  pub transaction_commit_latency_ms_p50: f64,
  pub transaction_commit_latency_ms_p95: f64,
  pub transaction_commit_latency_ms_p99: f64,

  pub disk_read_count: u64,
  pub disk_read_latency_micros_average: f64,
  pub disk_read_latency_micros_p50: f64,
  pub disk_write_count: u64,
  pub disk_write_latency_micros_average: f64,
  pub disk_write_latency_micros_p50: f64,
}
pub struct MetricsRegistry {
  pub buffer_pool_cache_hit: Counter,
  pub buffer_pool_cache_miss: Counter,
  pub buffer_pool_flush: Histogram,
  pub transaction_start_count: Counter,
  pub transaction_commit: Histogram,
  pub transaction_abort_count: Counter,
  pub disk_read: Histogram,
  pub disk_write: Histogram,
}
impl MetricsRegistry {
  pub fn new() -> Self {
    Self {
      buffer_pool_cache_hit: Counter::new(),
      buffer_pool_cache_miss: Counter::new(),
      buffer_pool_flush: Histogram::new(10, Duration::from_millis(1)),
      transaction_start_count: Counter::new(),
      transaction_commit: Histogram::new(1000, Duration::from_micros(10)),
      transaction_abort_count: Counter::new(),
      disk_read: Histogram::new(1000, Duration::from_nanos(1)),
      disk_write: Histogram::new(1000, Duration::from_nanos(1)),
    }
  }

  pub fn snapshot(&self) -> EngineMetrics {
    let transaction_commit = self.transaction_commit.snapshot();
    let buffer_pool = self.buffer_pool_flush.snapshot();
    let disk_read = self.disk_read.snapshot();
    let disk_write = self.disk_write.snapshot();

    EngineMetrics {
      buffer_pool_cache_hit: self.buffer_pool_cache_hit.load(),
      buffer_pool_cache_miss: self.buffer_pool_cache_miss.load(),
      buffer_pool_flush_count: buffer_pool.total_count(),
      buffer_pool_flush_latency_ms_average: buffer_pool.average(),

      transaction_start_count: self.transaction_start_count.load(),
      transaction_abort_count: self.transaction_abort_count.load(),

      transaction_commit_count: transaction_commit.total_count(),
      transaction_commit_latency_ms_average: transaction_commit.average() / 100.0,
      transaction_commit_latency_ms_p50: transaction_commit.percentile(0.5) / 100.0,
      transaction_commit_latency_ms_p95: transaction_commit.percentile(0.95) / 100.0,
      transaction_commit_latency_ms_p99: transaction_commit.percentile(0.99) / 100.0,

      disk_read_count: disk_read.total_count(),
      disk_read_latency_micros_average: disk_read.average() / 1_000.0,
      disk_read_latency_micros_p50: disk_read.percentile(0.5) / 1_000.0,
      disk_write_count: disk_write.total_count(),
      disk_write_latency_micros_average: disk_write.average() / 1_000.0,
      disk_write_latency_micros_p50: disk_write.percentile(0.5) / 1_000.0,
    }
  }
}
