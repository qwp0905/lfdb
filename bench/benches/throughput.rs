use std::time::Duration;

use bench::{engines, scenarios};
use criterion::{Criterion, criterion_group, criterion_main};
use tempfile::TempDir;

const DEFAULT_SAMPLE_SIZE: usize = 30;
const DEFAULT_CACHE_SIZE: usize = 512 << 20;

fn bench_sequential_get(c: &mut Criterion) {
  let group_name = "sequential-get";

  #[cfg(feature = "lfdb")]
  {
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(15));
    let dir = TempDir::new_in(".").expect("dir failed.");
    scenarios::throughput::sequential_get(group, || {
      engines::lfdb::new(DEFAULT_CACHE_SIZE, dir.path())
    });
  }

  #[cfg(feature = "redb")]
  {
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(15));
    let dir = TempDir::new_in(".").expect("dir failed.");
    scenarios::throughput::sequential_get(group, || {
      engines::redb::new(DEFAULT_CACHE_SIZE, dir.path())
    });
  }
}

fn bench_sequential_insert(c: &mut Criterion) {
  let group_name = "sequential-insert";

  #[cfg(feature = "lfdb")]
  {
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(15));
    let dir = TempDir::new_in(".").expect("dir failed.");
    scenarios::throughput::sequential_insert(group, || {
      engines::lfdb::new(DEFAULT_CACHE_SIZE, dir.path())
    });
  }

  #[cfg(feature = "redb")]
  {
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(15));
    let dir = TempDir::new_in(".").expect("dir failed.");
    scenarios::throughput::sequential_insert(group, || {
      engines::redb::new(DEFAULT_CACHE_SIZE, dir.path())
    });
  }
}

fn bench_sequential_update(c: &mut Criterion) {
  let group_name = "sequential-update";

  #[cfg(feature = "lfdb")]
  {
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(15));
    let dir = TempDir::new_in(".").expect("dir failed.");
    scenarios::throughput::sequential_update(group, || {
      engines::lfdb::new(DEFAULT_CACHE_SIZE, dir.path())
    });
  }

  #[cfg(feature = "redb")]
  {
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(15));
    let dir = TempDir::new_in(".").expect("dir failed.");
    scenarios::throughput::sequential_update(group, || {
      engines::redb::new(DEFAULT_CACHE_SIZE, dir.path())
    });
  }
}

fn bench_concurrent_get(c: &mut Criterion) {
  let group_name = "concurrent-get";

  #[cfg(feature = "lfdb")]
  {
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(15));
    let dir = TempDir::new_in(".").expect("dir failed.");
    scenarios::throughput::concurrent_get(group, || {
      engines::lfdb::new(DEFAULT_CACHE_SIZE, dir.path())
    });
  }

  #[cfg(feature = "redb")]
  {
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(15));
    let dir = TempDir::new_in(".").expect("dir failed.");
    scenarios::throughput::concurrent_get(group, || {
      engines::redb::new(DEFAULT_CACHE_SIZE, dir.path())
    });
  }
}

fn bench_concurrent_insert(c: &mut Criterion) {
  let group_name = "concurrent-insert";

  #[cfg(feature = "lfdb")]
  {
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(15));
    let dir = TempDir::new_in(".").expect("dir failed.");
    scenarios::throughput::concurrent_insert(group, || {
      engines::lfdb::new(DEFAULT_CACHE_SIZE, dir.path())
    });
  }

  #[cfg(feature = "redb")]
  {
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(15));
    let dir = TempDir::new_in(".").expect("dir failed.");
    scenarios::throughput::concurrent_insert(group, || {
      engines::redb::new(DEFAULT_CACHE_SIZE, dir.path())
    });
  }
}

fn bench_concurrent_update(c: &mut Criterion) {
  let group_name = "concurrent-update";

  #[cfg(feature = "lfdb")]
  {
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(15));
    let dir = TempDir::new_in(".").expect("dir failed.");
    scenarios::throughput::concurrent_update(group, || {
      engines::lfdb::new(DEFAULT_CACHE_SIZE, dir.path())
    });
  }

  #[cfg(feature = "redb")]
  {
    let mut group = c.benchmark_group(group_name);
    group
      .sample_size(DEFAULT_SAMPLE_SIZE)
      .measurement_time(Duration::from_secs(15));
    let dir = TempDir::new_in(".").expect("dir failed.");
    scenarios::throughput::concurrent_update(group, || {
      engines::redb::new(DEFAULT_CACHE_SIZE, dir.path())
    });
  }
}

criterion_group!(
  benches,
  bench_sequential_get,
  bench_sequential_insert,
  bench_sequential_update,
  bench_concurrent_get,
  bench_concurrent_insert,
  bench_concurrent_update,
);
criterion_main!(benches);
