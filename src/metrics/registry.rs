use std::time::{Duration, Instant};

use super::{Counter, Gauge, Histogram, TimeHistogram};

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
   * Number of block cache page reads total counts.
   */
  pub block_cache_read_count: u64,
  /**
   * Average block cache read latency in microseconds.
   */
  pub block_cache_read_latency_micros_avg: f64,
  /**
   * p50 block cache read latency in microseconds.
   */
  pub block_cache_read_latency_micros_p50: f64,
  /**
   * p95 block cache read latency in microseconds.
   */
  pub block_cache_read_latency_micros_p95: f64,
  /**
   * p99 block cache read latency in microseconds.
   */
  pub block_cache_read_latency_micros_p99: f64,
  /**
   * Number of reads served from the in-memory cache without hitting disk.
   */
  pub block_cache_hit: u64,
  /**
   * Number of block cache flush triggered counts.
   */
  pub checkpoint_cycle_count: u64,
  /**
   * Average block checkpoint cycle time in milliseconds.
   */
  pub checkpoint_cycle_time_ms_avg: f64,

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
   * Number of disk write batch block count.
   */
  pub disk_write_batch_count: u64,
  /**
   * Average disk write batch block count.
   */
  pub disk_write_batch_avg: f64,
  /**
   * p50 disk write batch block count.
   */
  pub disk_write_batch_p50: f64,

  /**
   * Number of disk fsync batch count.
   */
  pub disk_sync_batch_count: u64,
  /**
   * Average disk fsync batch count.
   */
  pub disk_sync_batch_avg: f64,
  /**
   * p50 disk fsync batch count.
   */
  pub disk_sync_batch_p50: f64,

  /**
   * Number of active IO threads processing disk requests.
   */
  pub active_io_threads: u64,

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

  /**
   * Number of btree splitted.
   */
  pub btree_split_count: u64,
}
impl std::fmt::Display for EngineMetrics {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let uptime = Duration::from_millis(self.uptime_ms);

    writeln!(
      f,
      "engine: uptime {:.1?} | active io threads {}",
      uptime, self.active_io_threads,
    )?;
    writeln!(
      f,
      "transactions: start {} | commit {} | abort {}",
      self.transaction_start_count,
      self.transaction_commit_count,
      self.transaction_abort_count,
    )?;
    writeln!(
      f,
      "tx duration: avg {:.3} ms | p50 {:.3} | p95 {:.3} | p99 {:.3}",
      self.transaction_duration_ms_avg,
      self.transaction_duration_ms_p50,
      self.transaction_duration_ms_p95,
      self.transaction_duration_ms_p99,
    )?;
    writeln!(
      f,
      "tx commit: avg {:.3} ms | p50 {:.3} | p95 {:.3} | p99 {:.3}",
      self.transaction_commit_latency_ms_avg,
      self.transaction_commit_latency_ms_p50,
      self.transaction_commit_latency_ms_p95,
      self.transaction_commit_latency_ms_p99,
    )?;
    writeln!(
      f,
      "get: count {} | avg {:.2} µs | p50 {:.2} | p95 {:.2} | p99 {:.2}",
      self.operation_get_count,
      self.operation_get_latency_micros_avg,
      self.operation_get_latency_micros_p50,
      self.operation_get_latency_micros_p95,
      self.operation_get_latency_micros_p99,
    )?;
    writeln!(
      f,
      "insert: count {} | avg {:.2} µs | p50 {:.2} | p95 {:.2} | p99 {:.2}",
      self.operation_insert_count,
      self.operation_insert_latency_micros_avg,
      self.operation_insert_latency_micros_p50,
      self.operation_insert_latency_micros_p95,
      self.operation_insert_latency_micros_p99,
    )?;
    writeln!(
      f,
      "remove: count {} | avg {:.2} µs | p50 {:.2} | p95 {:.2} | p99 {:.2}",
      self.operation_remove_count,
      self.operation_remove_latency_micros_avg,
      self.operation_remove_latency_micros_p50,
      self.operation_remove_latency_micros_p95,
      self.operation_remove_latency_micros_p99,
    )?;
    writeln!(
      f,
      "block cache: hit {} | read {}",
      self.block_cache_hit, self.block_cache_read_count,
    )?;
    writeln!(
      f,
      "cache read: avg {:.2} µs | p50 {:.2} | p95 {:.2} | p99 {:.2}",
      self.block_cache_read_latency_micros_avg,
      self.block_cache_read_latency_micros_p50,
      self.block_cache_read_latency_micros_p95,
      self.block_cache_read_latency_micros_p99,
    )?;
    writeln!(
      f,
      "disk read: count {} | avg {:.2} µs | p50 {:.2}",
      self.disk_read_count,
      self.disk_read_latency_micros_avg,
      self.disk_read_latency_micros_p50,
    )?;
    writeln!(
      f,
      "disk write: count {} | avg {:.2} µs | p50 {:.2}",
      self.disk_write_count,
      self.disk_write_latency_micros_avg,
      self.disk_write_latency_micros_p50,
    )?;
    writeln!(
      f,
      "disk write batch: count {} | avg {:.2} | p50 {:.2}",
      self.disk_write_batch_count, self.disk_write_batch_avg, self.disk_write_batch_p50,
    )?;
    writeln!(
      f,
      "sync batch: count {} | avg {:.2} | p50 {:.2}",
      self.disk_sync_batch_count, self.disk_sync_batch_avg, self.disk_sync_batch_p50,
    )?;
    writeln!(
      f,
      "checkpoint: cycles {} | avg {:.2} ms",
      self.checkpoint_cycle_count, self.checkpoint_cycle_time_ms_avg,
    )?;
    writeln!(f, "btree splitted: {}", self.btree_split_count)?;
    Ok(())
  }
}
pub struct MetricsRegistry {
  pub block_cache_read: TimeHistogram,
  pub block_cache_hit: Counter,

  pub checkpoint_cycle: TimeHistogram,

  pub disk_read: TimeHistogram,
  pub disk_write: TimeHistogram,
  pub active_io_threads: Gauge,

  pub disk_write_batch: Histogram,
  pub disk_sync_batch: Histogram,

  pub transaction_start: TimeHistogram,
  pub transaction_commit: TimeHistogram,
  pub transaction_abort_count: Counter,

  pub operation_get: TimeHistogram,
  pub operation_insert: TimeHistogram,
  pub operation_remove: TimeHistogram,

  pub btree_split: Counter,

  started_at: Instant,
}

const MICROS: Duration = Duration::from_micros(1);
const MILLIS: Duration = Duration::from_millis(1);

impl MetricsRegistry {
  pub fn new() -> Self {
    Self {
      block_cache_read: TimeHistogram::new(10_000, Duration::from_nanos(10)),
      block_cache_hit: Counter::new(),

      checkpoint_cycle: TimeHistogram::new(10, Duration::from_millis(1)),

      transaction_start: TimeHistogram::new(1000, Duration::from_micros(10)),
      transaction_commit: TimeHistogram::new(1000, Duration::from_micros(10)),
      transaction_abort_count: Counter::new(),

      disk_read: TimeHistogram::new(1000, Duration::from_nanos(100)),
      disk_write: TimeHistogram::new(1000, Duration::from_nanos(100)),
      disk_write_batch: Histogram::new(1000),
      disk_sync_batch: Histogram::new(1000),
      active_io_threads: Gauge::new(),

      operation_get: TimeHistogram::new(1000, Duration::from_nanos(100)),
      operation_insert: TimeHistogram::new(1000, Duration::from_nanos(100)),
      operation_remove: TimeHistogram::new(1000, Duration::from_nanos(100)),

      btree_split: Counter::new(),

      started_at: Instant::now(),
    }
  }

  pub fn snapshot(&self) -> EngineMetrics {
    let transaction_start = self.transaction_start.snapshot_with(MILLIS);
    let transaction_commit = self.transaction_commit.snapshot_with(MILLIS);
    let block_cache_read = self.block_cache_read.snapshot_with(MICROS);
    let checkpoint_cycle = self.checkpoint_cycle.snapshot_with(MILLIS);
    let disk_read = self.disk_read.snapshot_with(MICROS);
    let disk_write = self.disk_write.snapshot_with(MICROS);
    let operation_get = self.operation_get.snapshot_with(MICROS);
    let operation_insert = self.operation_insert.snapshot_with(MICROS);
    let operation_remove = self.operation_remove.snapshot_with(MICROS);

    let write_batch = self.disk_write_batch.snapshot();
    let sync_batch = self.disk_sync_batch.snapshot();

    EngineMetrics {
      uptime_ms: self.started_at.elapsed().as_millis() as u64,

      block_cache_read_count: block_cache_read.total_count(),
      block_cache_read_latency_micros_avg: block_cache_read.average(),
      block_cache_read_latency_micros_p50: block_cache_read.percentile(0.5),
      block_cache_read_latency_micros_p95: block_cache_read.percentile(0.95),
      block_cache_read_latency_micros_p99: block_cache_read.percentile(0.99),

      block_cache_hit: self.block_cache_hit.load(),

      checkpoint_cycle_count: checkpoint_cycle.total_count(),
      checkpoint_cycle_time_ms_avg: checkpoint_cycle.average(),

      disk_read_count: disk_read.total_count(),
      disk_read_latency_micros_avg: disk_read.average(),
      disk_read_latency_micros_p50: disk_read.percentile(0.5),

      disk_write_count: disk_write.total_count(),
      disk_write_latency_micros_avg: disk_write.average(),
      disk_write_latency_micros_p50: disk_write.percentile(0.5),

      disk_write_batch_count: write_batch.total_count(),
      disk_write_batch_avg: write_batch.average(),
      disk_write_batch_p50: write_batch.percentile(0.5),

      disk_sync_batch_count: sync_batch.total_count(),
      disk_sync_batch_avg: sync_batch.average(),
      disk_sync_batch_p50: sync_batch.percentile(0.5),

      active_io_threads: self.active_io_threads.load(),

      transaction_start_count: transaction_start.total_count(),
      transaction_abort_count: self.transaction_abort_count.load(),

      transaction_duration_ms_avg: transaction_start.average(),
      transaction_duration_ms_p50: transaction_start.percentile(0.5),
      transaction_duration_ms_p95: transaction_start.percentile(0.95),
      transaction_duration_ms_p99: transaction_start.percentile(0.99),

      transaction_commit_count: transaction_commit.total_count(),
      transaction_commit_latency_ms_avg: transaction_commit.average(),
      transaction_commit_latency_ms_p50: transaction_commit.percentile(0.5),
      transaction_commit_latency_ms_p95: transaction_commit.percentile(0.95),
      transaction_commit_latency_ms_p99: transaction_commit.percentile(0.99),

      operation_get_count: operation_get.total_count(),
      operation_get_latency_micros_avg: operation_get.average(),
      operation_get_latency_micros_p50: operation_get.percentile(0.5),
      operation_get_latency_micros_p95: operation_get.percentile(0.95),
      operation_get_latency_micros_p99: operation_get.percentile(0.99),

      operation_insert_count: operation_insert.total_count(),
      operation_insert_latency_micros_avg: operation_insert.average(),
      operation_insert_latency_micros_p50: operation_insert.percentile(0.5),
      operation_insert_latency_micros_p95: operation_insert.percentile(0.95),
      operation_insert_latency_micros_p99: operation_insert.percentile(0.99),

      operation_remove_count: operation_remove.total_count(),
      operation_remove_latency_micros_avg: operation_remove.average(),
      operation_remove_latency_micros_p50: operation_remove.percentile(0.5),
      operation_remove_latency_micros_p95: operation_remove.percentile(0.95),
      operation_remove_latency_micros_p99: operation_remove.percentile(0.99),

      btree_split_count: self.btree_split.load(),
    }
  }
}

#[cfg(test)]
#[path = "tests/registry.rs"]
mod tests;
