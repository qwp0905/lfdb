use std::{sync::Arc, time::Duration};

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use crossbeam::channel::{unbounded, Sender};
use lfkv_db::EngineBuilder;
use tempfile::TempDir;

const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 256;
const DEFAULT_SAMPLE_SIZE: usize = 30;
const SEQ_SIZE: usize = 1_000;
const CONC_SIZE: usize = 10_000;
const CONC_THREADS: usize = 128;

fn make_key(i: usize) -> Vec<u8> {
  format!("{i:0>width$}", width = KEY_SIZE)
    .as_bytes()
    .to_vec()
}

fn make_value(i: usize) -> Vec<u8> {
  let mut v = format!("{i:0>width$}", width = KEY_SIZE)
    .as_bytes()
    .to_vec();
  v.resize(VALUE_SIZE, b'x');
  v
}

fn build<T: AsRef<std::path::Path> + ?Sized>(dir: &T) -> EngineBuilder<&T> {
  EngineBuilder::new(dir)
    .group_commit_count(512)
    .buffer_pool_memory_capacity(512 << 20)
    .buffer_pool_shard_count(1 << 8)
    .wal_file_size(32 << 20)
    .gc_thread_count(5)
    .io_thread_count(5)
}

fn pre_write_keys<P: AsRef<std::path::Path> + ?Sized>(dir: &P, count: usize) {
  let engine = build(dir).build().unwrap();
  let mut tx = engine.new_tx().unwrap();
  (0..count)
    .map(|i| (make_key(i), make_value(i)))
    .for_each(|(k, v)| tx.insert(k, v).unwrap());
  tx.commit().unwrap();
}

fn bench_sequential_get(c: &mut Criterion) {
  let dir = TempDir::new_in(".").expect("dir failed.");
  pre_write_keys(dir.path(), SEQ_SIZE);

  let engine = build(dir.path()).build().expect("bootstrap error");

  let keys: Vec<_> = (0..SEQ_SIZE).map(make_key).collect();

  let mut group = c.benchmark_group("sequential-get");
  group
    .sample_size(DEFAULT_SAMPLE_SIZE)
    .measurement_time(Duration::from_secs(15))
    .throughput(Throughput::Elements(SEQ_SIZE as u64))
    .bench_function("bench", |b| {
      b.iter(|| {
        for i in 0..SEQ_SIZE {
          let mut tx = engine.new_tx().expect("start error");
          tx.get(&keys[i]).expect("get error");
          tx.commit().expect("commit error");
        }
      });
    });
  group.finish();
}

fn bench_sequential_insert(c: &mut Criterion) {
  let keys: Vec<_> = (0..SEQ_SIZE).map(make_key).collect();
  let values: Vec<_> = (0..SEQ_SIZE).map(make_value).collect();

  let mut group = c.benchmark_group("sequential-insert");
  group
    .sample_size(DEFAULT_SAMPLE_SIZE)
    .measurement_time(Duration::from_secs(15))
    .throughput(Throughput::Elements(SEQ_SIZE as u64))
    .bench_function("bench", |b| {
      b.iter_batched_ref(
        || {
          let dir = TempDir::new_in(".").expect("dir failed.");
          let engine = build(dir.path()).build().expect("bootstrap error");
          (dir, engine)
        },
        |(_, engine)| {
          for i in 0..SEQ_SIZE {
            let mut tx = engine.new_tx().expect("start error");
            tx.insert(keys[i].clone(), values[i].clone())
              .expect("insert error");
            tx.commit().expect("commit error");
          }
        },
        BatchSize::PerIteration,
      );
    });
  group.finish();
}

fn bench_sequential_update(c: &mut Criterion) {
  let dir = TempDir::new_in(".").expect("dir failed.");
  pre_write_keys(dir.path(), SEQ_SIZE);

  let engine = build(dir.path()).build().expect("bootstrap error");

  let keys: Vec<_> = (0..SEQ_SIZE).map(make_key).collect();
  let values: Vec<_> = (0..SEQ_SIZE).map(make_value).collect();

  let mut group = c.benchmark_group("sequential-update");
  group
    .sample_size(DEFAULT_SAMPLE_SIZE)
    .measurement_time(Duration::from_secs(15))
    .throughput(Throughput::Elements(SEQ_SIZE as u64))
    .bench_function("bench", |b| {
      b.iter(|| {
        for i in 0..SEQ_SIZE {
          let mut tx = engine.new_tx().expect("start error");
          tx.insert(keys[i].clone(), values[i].clone())
            .expect("update error");
          tx.commit().expect("commit error");
        }
      });
    });
  group.finish();
}

fn bench_concurrent_get(c: &mut Criterion) {
  let dir = TempDir::new_in(".").expect("dir failed.");
  pre_write_keys(dir.path(), CONC_SIZE);

  let engine = Arc::new(build(dir.path()).build().expect("bootstrap error"));
  let keys: Vec<_> = (0..CONC_SIZE).map(make_key).collect();
  let (tx, rx) = unbounded::<(Vec<u8>, Sender<()>)>();
  let threads: Vec<_> = (0..CONC_THREADS)
    .map(|_| {
      let rx = rx.clone();
      let e = engine.clone();
      std::thread::spawn(move || {
        while let Ok((k, done)) = rx.recv() {
          let mut tx = e.new_tx().expect("start error");
          tx.get(&k).expect("get error");
          tx.commit().expect("commit error");
          done.send(()).unwrap();
        }
      })
    })
    .collect();

  let mut group = c.benchmark_group("concurrent-get");
  group
    .sample_size(DEFAULT_SAMPLE_SIZE)
    .measurement_time(Duration::from_secs(20))
    .throughput(Throughput::Elements(CONC_SIZE as u64))
    .bench_function("bench", |b| {
      b.iter(|| {
        let mut waiting = Vec::with_capacity(CONC_SIZE);
        for i in 0..CONC_SIZE {
          let (t, r) = unbounded();
          tx.send((keys[i].clone(), t)).unwrap();
          waiting.push(r);
        }
        waiting.into_iter().for_each(|r| r.recv().unwrap());
      });
    });
  group.finish();

  drop(tx);
  threads.into_iter().for_each(|t| t.join().unwrap());
}

fn bench_concurrent_insert(c: &mut Criterion) {
  let keys: Vec<_> = (0..CONC_SIZE).map(make_key).collect();
  let values: Vec<_> = (0..CONC_SIZE).map(make_value).collect();

  let mut group = c.benchmark_group("concurrent-insert");
  group
    .sample_size(DEFAULT_SAMPLE_SIZE)
    .measurement_time(Duration::from_secs(20))
    .throughput(Throughput::Elements(CONC_SIZE as u64))
    .bench_function("bench", |b| {
      b.iter_batched_ref(
        || {
          let dir = TempDir::new_in(".").expect("dir failed.");
          let engine = Arc::new(build(dir.path()).build().expect("bootstrap error"));
          let (tx, rx) = unbounded::<(Vec<u8>, Vec<u8>, Sender<()>)>();
          let threads: Vec<_> = (0..CONC_THREADS)
            .map(|_| {
              let rx = rx.clone();
              let e = engine.clone();
              std::thread::spawn(move || {
                while let Ok((k, v, done)) = rx.recv() {
                  let mut tx = e.new_tx().expect("start error");
                  tx.insert(k, v).expect("insert error");
                  tx.commit().expect("commit error");
                  done.send(()).unwrap();
                }
              })
            })
            .collect();
          (dir, engine, tx, threads)
        },
        |(_, _, tx, _)| {
          let mut waiting = Vec::with_capacity(CONC_SIZE);
          for i in 0..CONC_SIZE {
            let (t, r) = unbounded();
            tx.send((keys[i].clone(), values[i].clone(), t)).unwrap();
            waiting.push(r);
          }
          waiting.into_iter().for_each(|r| r.recv().unwrap());
        },
        BatchSize::PerIteration,
      );
    });
  group.finish();
}

fn bench_concurrent_update(c: &mut Criterion) {
  let dir = TempDir::new_in(".").expect("dir failed.");
  pre_write_keys(dir.path(), CONC_SIZE);

  let engine = Arc::new(build(dir.path()).build().expect("bootstrap error"));
  let keys: Vec<_> = (0..CONC_SIZE).map(make_key).collect();
  let values: Vec<_> = (0..CONC_SIZE).map(make_value).collect();
  let (tx, rx) = unbounded::<(Vec<u8>, Vec<u8>, Sender<()>)>();
  let threads: Vec<_> = (0..CONC_THREADS)
    .map(|_| {
      let rx = rx.clone();
      let e = engine.clone();
      std::thread::spawn(move || {
        while let Ok((k, v, done)) = rx.recv() {
          let mut tx = e.new_tx().expect("start error");
          tx.insert(k, v).expect("update error");
          tx.commit().expect("commit error");
          done.send(()).unwrap();
        }
      })
    })
    .collect();

  let mut group = c.benchmark_group("concurrent-update");
  group
    .sample_size(DEFAULT_SAMPLE_SIZE)
    .measurement_time(Duration::from_secs(20))
    .throughput(Throughput::Elements(CONC_SIZE as u64))
    .bench_function("bench", |b| {
      b.iter(|| {
        let mut waiting = Vec::with_capacity(CONC_SIZE);
        for i in 0..CONC_SIZE {
          let (t, r) = unbounded();
          tx.send((keys[i].clone(), values[i].clone(), t)).unwrap();
          waiting.push(r);
        }
        waiting.into_iter().for_each(|r| r.recv().unwrap());
      });
    });
  group.finish();

  drop(tx);
  threads.into_iter().for_each(|t| t.join().unwrap());
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
