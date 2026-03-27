use std::time::{Duration, Instant};

use super::{Counter, Histogram};

/**
 * A point-in-time snapshot of engine metrics. Obtain via Engine::metrics().
 */
#[derive(Debug)]
pub struct EngineMetrics {
  /**
   * Milliseconds since the engine started.
   */
  pub uptime_ms: u64,

  /**
   * Number of buffer pool page reads total counts.
   */
  pub buffer_pool_read_count: u64,
  /**
   * Average buffer pool read latency in microseconds.
   */
  pub buffer_pool_read_latency_micros_avg: f64,
  /**
   * p50 buffer pool read latency in microseconds.
   */
  pub buffer_pool_read_latency_micros_p50: f64,
  /**
   * p95 buffer pool read latency in microseconds.
   */
  pub buffer_pool_read_latency_micros_p95: f64,
  /**
   * p99 buffer pool read latency in microseconds.
   */
  pub buffer_pool_read_latency_micros_p99: f64,
  /**
   * Number of reads served from the in-memory cache without hitting disk.
   */
  pub buffer_pool_cache_hit: u64,
  /**
   * Number of buffer pool flush triggered counts.
   */
  pub buffer_pool_flush_count: u64,
  /**
   * Average buffer pool flush latency in milliseconds.
   */
  pub buffer_pool_flush_latency_ms_avg: f64,

  /**
   * Number of disk read io counts.
   */
  pub disk_read_count: u64,
  /**
   * Average disk read io latency in microseconds.
   */
  pub disk_read_latency_micros_avg: f64,
  /**
   * p50 disk read io latency in microseconds.
   */
  pub disk_read_latency_micros_p50: f64,
  /**
   * Number of disk write io counts.
   */
  pub disk_write_count: u64,
  /**
   * Average disk write io latency in microseconds.
   */
  pub disk_write_latency_micros_avg: f64,
  /**
   * p50 disk write io latency in microseconds.
   */
  pub disk_write_latency_micros_p50: f64,

  /**
   * Number of transactions started.
   */
  pub transaction_start_count: u64,
  /**
   * Number of transactions aborted (explicitly or by timeout).
   */
  pub transaction_abort_count: u64,

  /**
   * Average total transaction duration from start to end in milliseconds.
   */
  pub transaction_duration_ms_avg: f64,
  /**
   * p50 transaction duration in milliseconds.
   */
  pub transaction_duration_ms_p50: f64,
  /**
   * p95 transaction duration in milliseconds.
   */
  pub transaction_duration_ms_p95: f64,
  /**
   * p99 transaction duration in milliseconds.
   */
  pub transaction_duration_ms_p99: f64,

  /**
   * Number of successfully committed transactions.
   */
  pub transaction_commit_count: u64,
  /**
   * Average commit latency (WAL flush) in milliseconds.
   */
  pub transaction_commit_latency_ms_avg: f64,
  /**
   * p50 commit latency in milliseconds.
   */
  pub transaction_commit_latency_ms_p50: f64,
  /**
   * p95 commit latency in milliseconds.
   */
  pub transaction_commit_latency_ms_p95: f64,
  /**
   * p99 commit latency in milliseconds.
   */
  pub transaction_commit_latency_ms_p99: f64,

  /**
   * Number of get operations.
   */
  pub operation_get_count: u64,
  /**
   * Average get operation latency in microseconds.
   */
  pub operation_get_latency_micros_avg: f64,
  /**
   * p50 get operation latency in microseconds.
   */
  pub operation_get_latency_micros_p50: f64,
  /**
   * p95 get operation latency in microseconds.
   */
  pub operation_get_latency_micros_p95: f64,
  /**
   * p99 get operation latency in microseconds.
   */
  pub operation_get_latency_micros_p99: f64,

  /**
   * Number of insert operations.
   */
  pub operation_insert_count: u64,
  /**
   * Average insert operation latency in microseconds.
   */
  pub operation_insert_latency_micros_avg: f64,
  /**
   * p50 insert operation latency in microseconds.
   */
  pub operation_insert_latency_micros_p50: f64,
  /**
   * p95 insert operation latency in microseconds.
   */
  pub operation_insert_latency_micros_p95: f64,
  /**
   * p99 insert operation latency in microseconds.
   */
  pub operation_insert_latency_micros_p99: f64,

  /**
   * Number of remove operations.
   */
  pub operation_remove_count: u64,
  /**
   * Average remove operation latency in microseconds.
   */
  pub operation_remove_latency_micros_avg: f64,
  /**
   * p50 remove operation latency in microseconds.
   */
  pub operation_remove_latency_micros_p50: f64,
  /**
   * p95 remove operation latency in microseconds.
   */
  pub operation_remove_latency_micros_p95: f64,
  /**
   * p99 remove operation latency in microseconds.
   */
  pub operation_remove_latency_micros_p99: f64,
}
pub struct MetricsRegistry {
  pub buffer_pool_read: Histogram,
  pub buffer_pool_cache_hit: Counter,
  pub buffer_pool_flush: Histogram,

  pub disk_read: Histogram,
  pub disk_write: Histogram,

  pub transaction_start: Histogram,
  pub transaction_commit: Histogram,
  pub transaction_abort_count: Counter,

  pub operation_get: Histogram,
  pub operation_insert: Histogram,
  pub operation_remove: Histogram,

  started_at: Instant,
}
impl MetricsRegistry {
  pub fn new() -> Self {
    Self {
      buffer_pool_read: Histogram::new(1000, Duration::from_nanos(100)),
      buffer_pool_cache_hit: Counter::new(),
      buffer_pool_flush: Histogram::new(10, Duration::from_millis(1)),
      transaction_start: Histogram::new(1000, Duration::from_micros(10)),
      transaction_commit: Histogram::new(1000, Duration::from_micros(10)),
      transaction_abort_count: Counter::new(),
      disk_read: Histogram::new(1000, Duration::from_nanos(100)),
      disk_write: Histogram::new(1000, Duration::from_nanos(100)),
      started_at: Instant::now(),
      operation_get: Histogram::new(1000, Duration::from_nanos(100)),
      operation_insert: Histogram::new(1000, Duration::from_nanos(100)),
      operation_remove: Histogram::new(1000, Duration::from_nanos(100)),
    }
  }

  pub fn snapshot(&self) -> EngineMetrics {
    let transaction_start = self.transaction_start.snapshot();
    let transaction_commit = self.transaction_commit.snapshot();
    let buffer_pool_read = self.buffer_pool_read.snapshot();
    let buffer_pool_flush = self.buffer_pool_flush.snapshot();
    let disk_read = self.disk_read.snapshot();
    let disk_write = self.disk_write.snapshot();
    let operation_get = self.operation_get.snapshot();
    let operation_insert = self.operation_insert.snapshot();
    let operation_remove = self.operation_remove.snapshot();

    EngineMetrics {
      uptime_ms: self.started_at.elapsed().as_millis() as u64,

      buffer_pool_read_count: buffer_pool_read.total_count(),
      buffer_pool_read_latency_micros_avg: buffer_pool_read.average() / 10.0,
      buffer_pool_read_latency_micros_p50: buffer_pool_read.percentile(0.5) / 10.0,
      buffer_pool_read_latency_micros_p95: buffer_pool_read.percentile(0.95) / 10.0,
      buffer_pool_read_latency_micros_p99: buffer_pool_read.percentile(0.99) / 10.0,

      buffer_pool_cache_hit: self.buffer_pool_cache_hit.load(),

      buffer_pool_flush_count: buffer_pool_flush.total_count(),
      buffer_pool_flush_latency_ms_avg: buffer_pool_flush.average(),

      disk_read_count: disk_read.total_count(),
      disk_read_latency_micros_avg: disk_read.average() / 10.0,
      disk_read_latency_micros_p50: disk_read.percentile(0.5) / 10.0,
      disk_write_count: disk_write.total_count(),
      disk_write_latency_micros_avg: disk_write.average() / 10.0,
      disk_write_latency_micros_p50: disk_write.percentile(0.5) / 10.0,

      transaction_start_count: transaction_start.total_count(),
      transaction_abort_count: self.transaction_abort_count.load(),

      transaction_duration_ms_avg: transaction_start.average() / 100.0,
      transaction_duration_ms_p50: transaction_start.percentile(0.5) / 100.0,
      transaction_duration_ms_p95: transaction_start.percentile(0.95) / 100.0,
      transaction_duration_ms_p99: transaction_start.percentile(0.99) / 100.0,

      transaction_commit_count: transaction_commit.total_count(),
      transaction_commit_latency_ms_avg: transaction_commit.average() / 100.0,
      transaction_commit_latency_ms_p50: transaction_commit.percentile(0.5) / 100.0,
      transaction_commit_latency_ms_p95: transaction_commit.percentile(0.95) / 100.0,
      transaction_commit_latency_ms_p99: transaction_commit.percentile(0.99) / 100.0,

      operation_get_count: operation_get.total_count(),
      operation_get_latency_micros_avg: operation_get.average() / 10.0,
      operation_get_latency_micros_p50: operation_get.percentile(0.5) / 10.0,
      operation_get_latency_micros_p95: operation_get.percentile(0.95) / 10.0,
      operation_get_latency_micros_p99: operation_get.percentile(0.99) / 10.0,

      operation_insert_count: operation_insert.total_count(),
      operation_insert_latency_micros_avg: operation_insert.average() / 10.0,
      operation_insert_latency_micros_p50: operation_insert.percentile(0.5) / 10.0,
      operation_insert_latency_micros_p95: operation_insert.percentile(0.95) / 10.0,
      operation_insert_latency_micros_p99: operation_insert.percentile(0.99) / 10.0,

      operation_remove_count: operation_remove.total_count(),
      operation_remove_latency_micros_avg: operation_remove.average() / 10.0,
      operation_remove_latency_micros_p50: operation_remove.percentile(0.5) / 10.0,
      operation_remove_latency_micros_p95: operation_remove.percentile(0.95) / 10.0,
      operation_remove_latency_micros_p99: operation_remove.percentile(0.99) / 10.0,
    }
  }
}
