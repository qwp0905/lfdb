use std::time::Duration;

use bench::scenarios;
use criterion::{Criterion, criterion_group, criterion_main};
use tempfile::TempDir;

const DEFAULT_RECORD_COUNT: usize = 500_000;

fn record_count() -> usize {
  std::env::var("YCSB_RECORD_COUNT")
    .ok()
    .and_then(|v| v.parse().ok())
    .unwrap_or(DEFAULT_RECORD_COUNT)
}
const DEFAULT_OP_COUNT: usize = 300_000;

fn op_count() -> usize {
  std::env::var("YCSB_OP_COUNT")
    .ok()
    .and_then(|v| v.parse().ok())
    .unwrap_or(DEFAULT_OP_COUNT)
}
const THREADS: usize = 128;

fn thread_count() -> usize {
  std::env::var("YCSB_THREAD_COUNT")
    .ok()
    .and_then(|v| v.parse().ok())
    .unwrap_or(THREADS)
}
const DEFAULT_MEASURE_TIME: Duration = Duration::from_secs(300);

fn cache_size() -> usize {
  std::env::var("YCSB_CACHE_SIZE")
    .ok()
    .and_then(|v| v.parse().ok())
    .unwrap_or(DEFAULT_CACHE_SIZE)
    << 20
}
const DEFAULT_CACHE_SIZE: usize = 1024;

/// Workload A: 50% read, 50% update (write-heavy, session store)
fn bench_ycsb_a(c: &mut Criterion) {
  let record_count = record_count();
  let op_count = op_count();
  let thread_count = thread_count();
  let group_name = "ycsb-a";

  #[cfg(feature = "lfdb")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("lfdb/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_a::<lfdb::Engine>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }

  #[cfg(feature = "redb")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("redb/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_a::<redb::Database>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }

  #[cfg(feature = "rocksdb")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("rocksdb/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_a::<bench::engines::rocksdb::Engine>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }

  #[cfg(feature = "sled")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("sled/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_a::<sled::Db>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }
}

/// Workload B: 95% read, 5% update (read-heavy, typical web app)
fn bench_ycsb_b(c: &mut Criterion) {
  let record_count = record_count();
  let op_count = op_count();
  let thread_count = thread_count();
  let group_name = "ycsb-b";

  #[cfg(feature = "lfdb")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("lfdb/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_b::<lfdb::Engine>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }

  #[cfg(feature = "redb")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("redb/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_b::<redb::Database>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }

  #[cfg(feature = "rocksdb")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("rocksdb/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_b::<bench::engines::rocksdb::Engine>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }

  #[cfg(feature = "sled")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("sled/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_b::<sled::Db>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }
}

/// Workload D: 95% read, 5% insert (read latest, timeline/feed)
fn bench_ycsb_d(c: &mut Criterion) {
  let record_count = record_count();
  let op_count = op_count();
  let thread_count = thread_count();
  let group_name = "ycsb-d";

  #[cfg(feature = "lfdb")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("lfdb/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_d::<lfdb::Engine>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }

  #[cfg(feature = "redb")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("redb/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_d::<redb::Database>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }

  #[cfg(feature = "rocksdb")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("rocksdb/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_d::<bench::engines::rocksdb::Engine>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }

  #[cfg(feature = "sled")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("sled/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_d::<sled::Db>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }
}

/// Workload E: 95% scan, 5% insert (range query heavy, analytics)
fn bench_ycsb_e(c: &mut Criterion) {
  let record_count = record_count();
  let op_count = op_count();
  let thread_count = thread_count();
  let group_name = "ycsb-e";

  #[cfg(feature = "lfdb")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("lfdb/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_e::<lfdb::Engine>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }

  #[cfg(feature = "redb")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("redb/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_e::<redb::Database>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }

  #[cfg(feature = "rocksdb")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("rocksdb/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_e::<bench::engines::rocksdb::Engine>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }

  #[cfg(feature = "sled")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("sled/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_e::<sled::Db>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }
}

/// Workload F: 50% read, 50% read-modify-write (transactional, account balance)
fn bench_ycsb_f(c: &mut Criterion) {
  let record_count = record_count();
  let op_count = op_count();
  let thread_count = thread_count();
  let group_name = "ycsb-f";

  #[cfg(feature = "lfdb")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("lfdb/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_f::<lfdb::Engine>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }

  #[cfg(feature = "redb")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("redb/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_f::<redb::Database>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }

  #[cfg(feature = "rocksdb")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("rocksdb/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_f::<bench::engines::rocksdb::Engine>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
  }

  #[cfg(feature = "sled")]
  {
    let dir = TempDir::new_in("..").expect("dir failed.");
    let mut group = c.benchmark_group(format!("sled/{group_name}"));
    group.measurement_time(DEFAULT_MEASURE_TIME);
    scenarios::ycsb::workload_f::<sled::Db>(
      dir.path(),
      cache_size(),
      record_count,
      op_count,
      thread_count,
      group,
    );
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
