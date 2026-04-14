use std::time::Duration;

use bench::{engines, scenarios};
use criterion::{Criterion, criterion_group, criterion_main};
use tempfile::TempDir;

const DEFAULT_RECORD_COUNT: usize = 500_000;

fn record_count() -> usize {
  std::env::var("YCSB_RECORD_COUNT")
    .ok()
    .and_then(|v| v.parse().ok())
    .unwrap_or(DEFAULT_RECORD_COUNT)
}
const DEFAULT_OP_COUNT: usize = 150_000;

fn op_count() -> usize {
  std::env::var("YCSB_OP_COUNT")
    .ok()
    .and_then(|v| v.parse().ok())
    .unwrap_or(DEFAULT_OP_COUNT)
}
const THREADS: usize = 128;
const DEFAULT_SAMPLE_SIZE: usize = 10;

const CACHE_SIZE: usize = 1024 << 20;

/// Workload A: 50% read, 50% update (write-heavy, session store)
fn bench_ycsb_a(c: &mut Criterion) {
  let record_count = record_count();
  let op_count = op_count();
  let group_name = "ycsb-a";

  #[cfg(feature = "lfdb")]
  {
    let dir = TempDir::new_in(".").expect("dir failed.");
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(20));
    scenarios::ycsb::workload_a(record_count, op_count, THREADS, group, || {
      engines::lfdb::new(CACHE_SIZE, dir.path())
    });
  }

  #[cfg(feature = "redb")]
  {
    let dir = TempDir::new_in(".").expect("dir failed.");
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(20));
    scenarios::ycsb::workload_a(record_count, op_count, THREADS, group, || {
      engines::redb::new(CACHE_SIZE, dir.path())
    });
  }
}

/// Workload B: 95% read, 5% update (read-heavy, typical web app)
fn bench_ycsb_b(c: &mut Criterion) {
  let record_count = record_count();
  let op_count = op_count();
  let group_name = "ycsb-b";

  #[cfg(feature = "lfdb")]
  {
    let dir = TempDir::new_in(".").expect("dir failed.");
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(20));
    scenarios::ycsb::workload_b(record_count, op_count, THREADS, group, || {
      engines::lfdb::new(CACHE_SIZE, dir.path())
    });
  }

  #[cfg(feature = "redb")]
  {
    let dir = TempDir::new_in(".").expect("dir failed.");
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(20));
    scenarios::ycsb::workload_b(record_count, op_count, THREADS, group, || {
      engines::redb::new(CACHE_SIZE, dir.path())
    });
  }
}

/// Workload D: 95% read, 5% insert (read latest, timeline/feed)
fn bench_ycsb_d(c: &mut Criterion) {
  let record_count = record_count();
  let op_count = op_count();
  let group_name = "ycsb-d";

  #[cfg(feature = "lfdb")]
  {
    let dir = TempDir::new_in(".").expect("dir failed.");
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(20));
    scenarios::ycsb::workload_d(record_count, op_count, THREADS, group, || {
      engines::lfdb::new(CACHE_SIZE, dir.path())
    });
  }

  #[cfg(feature = "redb")]
  {
    let dir = TempDir::new_in(".").expect("dir failed.");
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(20));
    scenarios::ycsb::workload_d(record_count, op_count, THREADS, group, || {
      engines::redb::new(CACHE_SIZE, dir.path())
    });
  }
}

/// Workload E: 95% scan, 5% insert (range query heavy, analytics)
fn bench_ycsb_e(c: &mut Criterion) {
  let record_count = record_count();
  let op_count = op_count();
  let group_name = "ycsb-e";

  #[cfg(feature = "lfdb")]
  {
    let dir = TempDir::new_in(".").expect("dir failed.");
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(20));
    scenarios::ycsb::workload_e(record_count, op_count, THREADS, group, || {
      engines::lfdb::new(CACHE_SIZE, dir.path())
    });
  }

  #[cfg(feature = "redb")]
  {
    let dir = TempDir::new_in(".").expect("dir failed.");
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(20));
    scenarios::ycsb::workload_e(record_count, op_count, THREADS, group, || {
      engines::redb::new(CACHE_SIZE, dir.path())
    });
  }
}

/// Workload F: 50% read, 50% read-modify-write (transactional, account balance)
fn bench_ycsb_f(c: &mut Criterion) {
  let record_count = record_count();
  let op_count = op_count();
  let group_name = "ycsb-f";

  #[cfg(feature = "lfdb")]
  {
    let dir = TempDir::new_in(".").expect("dir failed.");
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(20));
    scenarios::ycsb::workload_f(record_count, op_count, THREADS, group, || {
      engines::lfdb::new(CACHE_SIZE, dir.path())
    });
  }

  #[cfg(feature = "redb")]
  {
    let dir = TempDir::new_in(".").expect("dir failed.");
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(20));
    scenarios::ycsb::workload_f(record_count, op_count, THREADS, group, || {
      engines::redb::new(CACHE_SIZE, dir.path())
    });
  }
}

criterion_group!(
  ycsb,
  bench_ycsb_a,
  bench_ycsb_b,
  bench_ycsb_d,
  bench_ycsb_e,
  bench_ycsb_f
);
criterion_main!(ycsb);
