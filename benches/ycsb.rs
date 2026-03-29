use std::{
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
  time::Duration,
};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use crossbeam::channel::{unbounded, Sender};
use lfdb::EngineBuilder;
use rand::Rng;
use rand_distr::{Distribution, Zipf};
use tempfile::TempDir;

const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 256;
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
const SCAN_LENGTH: usize = 100;
const ZIPF_EXPONENT: f64 = 0.99;
const DEFAULT_SAMPLE_SIZE: usize = 10;

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
    .buffer_pool_memory_capacity(1024 << 20)
    .buffer_pool_shard_count(1 << 3)
    .wal_file_size(512 << 20)
    .gc_thread_count(5)
}

fn pre_load(dir: &std::path::Path, count: usize) {
  let engine = build(dir).build().unwrap();
  let mut tx = engine.new_tx_timeout(Duration::from_mins(10)).unwrap();
  (0..count)
    .map(|i| (make_key(i), make_value(i)))
    .for_each(|(k, v)| tx.insert(k, v).unwrap());
  tx.commit().unwrap();
}

enum Op {
  Get(Vec<u8>),
  Insert(Vec<u8>, Vec<u8>),
  Scan(Vec<u8>, Vec<u8>),
  ReadModifyWrite(Vec<u8>, Vec<u8>),
}

fn spawn_workers(
  engine: Arc<lfdb::Engine>,
  count: usize,
  done: &Sender<()>,
) -> (
  Sender<Op>,
  Arc<AtomicUsize>,
  Vec<std::thread::JoinHandle<()>>,
) {
  let (tx, rx) = unbounded::<Op>();
  let c = Arc::new(AtomicUsize::new(0));
  let threads = (0..count)
    .map(|_| {
      let rx = rx.clone();
      let e = engine.clone();
      let done = done.clone();
      let c = c.clone();
      std::thread::spawn(move || {
        while let Ok(op) = rx.recv() {
          loop {
            let mut tx = e.new_tx().expect("start error");
            let conflict = match &op {
              Op::Get(k) => {
                tx.get(k).expect("get error");
                false
              }
              Op::Insert(k, v) => match tx.insert(k.clone(), v.clone()) {
                Ok(_) => false,
                Err(lfdb::Error::WriteConflict) => true,
                Err(e) => panic!("insert error: {e}"),
              },
              Op::Scan(start, end) => {
                let mut iter = tx.scan(start, end).expect("scan error");
                while let Ok(Some(_)) = iter.try_next() {}
                false
              }
              Op::ReadModifyWrite(k, v) => {
                tx.get(k).expect("rmw read error");
                match tx.insert(k.clone(), v.clone()) {
                  Ok(_) => false,
                  Err(lfdb::Error::WriteConflict) => true,
                  Err(e) => panic!("rmw write error: {e}"),
                }
              }
            };
            if conflict {
              continue;
            }
            tx.commit().expect("commit error");
            break;
          }
          if c.fetch_sub(1, Ordering::Release) == 1 {
            done.send(()).unwrap();
          }
        }
      })
    })
    .collect();
  (tx, c, threads)
}

fn zipfian_index(rng: &mut impl Rng, zipf: &Zipf<f64>, record_count: usize) -> usize {
  let sample: f64 = zipf.sample(rng);
  (sample as usize).saturating_sub(1).min(record_count - 1)
}

/// Workload A: 50% read, 50% update (write-heavy, session store)
fn bench_ycsb_a(c: &mut Criterion) {
  let record_count = record_count();
  let op_count = op_count();
  let dir = TempDir::new_in(".").expect("dir failed.");
  pre_load(dir.path(), record_count);

  let engine = Arc::new(build(dir.path()).build().expect("bootstrap error"));
  let (t, r) = unbounded();
  let (tx, counter, threads) = spawn_workers(engine.clone(), THREADS, &t);
  let zipf = Zipf::new(record_count as u64, ZIPF_EXPONENT).unwrap();
  let rng = &mut rand::thread_rng();

  let mut group = c.benchmark_group("ycsb-a");
  group
    .sample_size(DEFAULT_SAMPLE_SIZE)
    .measurement_time(Duration::from_secs(20))
    .throughput(Throughput::Elements(op_count as u64))
    .bench_function("50read-50update", |b| {
      b.iter(|| {
        counter.store(op_count, Ordering::Release);
        for _ in 0..op_count {
          let idx = zipfian_index(rng, &zipf, record_count);
          let key = make_key(idx);
          let op = if rng.gen_bool(0.50) {
            Op::Get(key)
          } else {
            Op::Insert(key, make_value(idx))
          };
          tx.send(op).unwrap();
        }
        r.recv().unwrap();
      });
    });
  group.finish();

  drop(tx);
  threads.into_iter().for_each(|t| t.join().unwrap());
}

/// Workload B: 95% read, 5% update (read-heavy, typical web app)
fn bench_ycsb_b(c: &mut Criterion) {
  let record_count = record_count();
  let op_count = op_count();
  let dir = TempDir::new_in(".").expect("dir failed.");
  pre_load(dir.path(), record_count);

  let engine = Arc::new(build(dir.path()).build().expect("bootstrap error"));
  let (t, r) = unbounded();
  let (tx, counter, threads) = spawn_workers(engine.clone(), THREADS, &t);
  let zipf = Zipf::new(record_count as u64, ZIPF_EXPONENT).unwrap();
  let rng = &mut rand::thread_rng();

  let mut group = c.benchmark_group("ycsb-b");
  group
    .sample_size(DEFAULT_SAMPLE_SIZE)
    .measurement_time(Duration::from_secs(20))
    .throughput(Throughput::Elements(op_count as u64))
    .bench_function("95read-5update", |b| {
      b.iter(|| {
        counter.store(op_count, Ordering::Release);
        for _ in 0..op_count {
          let idx = zipfian_index(rng, &zipf, record_count);
          let key = make_key(idx);
          let op = if rng.gen_bool(0.95) {
            Op::Get(key)
          } else {
            Op::Insert(key, make_value(idx))
          };
          tx.send(op).unwrap();
        }
        r.recv().unwrap();
      });
    });
  group.finish();

  drop(tx);
  threads.into_iter().for_each(|t| t.join().unwrap());
}

/// Workload D: 95% read, 5% insert (read latest, timeline/feed)
fn bench_ycsb_d(c: &mut Criterion) {
  let record_count = record_count();
  let op_count = op_count();
  let dir = TempDir::new_in(".").expect("dir failed.");
  pre_load(dir.path(), record_count);

  let engine = Arc::new(build(dir.path()).build().expect("bootstrap error"));
  let (t, r) = unbounded();
  let (tx, counter, threads) = spawn_workers(engine.clone(), THREADS, &t);
  let insert_counter = AtomicUsize::new(record_count);
  let rng = &mut rand::thread_rng();

  let mut group = c.benchmark_group("ycsb-d");
  group
    .sample_size(DEFAULT_SAMPLE_SIZE)
    .measurement_time(Duration::from_secs(20))
    .throughput(Throughput::Elements(op_count as u64))
    .bench_function("95read-5insert-latest", |b| {
      b.iter(|| {
        counter.store(op_count, Ordering::Release);
        let latest = insert_counter.load(Ordering::Relaxed);
        for _ in 0..op_count {
          let op = if rng.gen_bool(0.95) {
            let idx = latest.saturating_sub(rng.gen_range(0..latest.min(1000)));
            Op::Get(make_key(idx))
          } else {
            let idx = insert_counter.fetch_add(1, Ordering::Relaxed);
            Op::Insert(make_key(idx), make_value(idx))
          };
          tx.send(op).unwrap();
        }
        r.recv().unwrap();
      });
    });
  group.finish();

  drop(tx);
  threads.into_iter().for_each(|t| t.join().unwrap());
}

/// Workload E: 95% scan, 5% insert (range query heavy, analytics)
fn bench_ycsb_e(c: &mut Criterion) {
  let record_count = record_count();
  let op_count = op_count();
  let dir = TempDir::new_in(".").expect("dir failed.");
  pre_load(dir.path(), record_count);

  let engine = Arc::new(build(dir.path()).build().expect("bootstrap error"));
  let (t, r) = unbounded();
  let (tx, counter, threads) = spawn_workers(engine.clone(), THREADS, &t);
  let insert_counter = AtomicUsize::new(record_count);
  let rng = &mut rand::thread_rng();

  let mut group = c.benchmark_group("ycsb-e");
  group
    .sample_size(DEFAULT_SAMPLE_SIZE)
    .measurement_time(Duration::from_secs(20))
    .throughput(Throughput::Elements(op_count as u64))
    .bench_function("95scan-5insert", |b| {
      b.iter(|| {
        let current = insert_counter.load(Ordering::Relaxed);
        let zipf = Zipf::new(current as u64, ZIPF_EXPONENT).unwrap();
        counter.store(op_count, Ordering::Release);
        for _ in 0..op_count {
          let op = if rng.gen_bool(0.95) {
            let idx = zipfian_index(rng, &zipf, current);
            let start = make_key(idx);
            let end = make_key((idx + SCAN_LENGTH).min(current - 1));
            Op::Scan(start, end)
          } else {
            let idx = insert_counter.fetch_add(1, Ordering::Relaxed);
            Op::Insert(make_key(idx), make_value(idx))
          };
          tx.send(op).unwrap();
        }
        r.recv().unwrap();
      });
    });
  group.finish();

  drop(tx);
  threads.into_iter().for_each(|t| t.join().unwrap());
}

/// Workload F: 50% read, 50% read-modify-write (transactional, account balance)
fn bench_ycsb_f(c: &mut Criterion) {
  let record_count = record_count();
  let op_count = op_count();
  let dir = TempDir::new_in(".").expect("dir failed.");
  pre_load(dir.path(), record_count);

  let engine = Arc::new(build(dir.path()).build().expect("bootstrap error"));
  let (t, r) = unbounded();
  let (tx, counter, threads) = spawn_workers(engine.clone(), THREADS, &t);
  let zipf = Zipf::new(record_count as u64, ZIPF_EXPONENT).unwrap();
  let rng = &mut rand::thread_rng();

  let mut group = c.benchmark_group("ycsb-f");
  group
    .sample_size(DEFAULT_SAMPLE_SIZE)
    .measurement_time(Duration::from_secs(20))
    .throughput(Throughput::Elements(op_count as u64))
    .bench_function("50read-50rmw", |b| {
      b.iter(|| {
        counter.store(op_count, Ordering::Release);
        for _ in 0..op_count {
          let idx = zipfian_index(rng, &zipf, record_count);
          let key = make_key(idx);
          let op = if rng.gen_bool(0.50) {
            Op::Get(key)
          } else {
            Op::ReadModifyWrite(key, make_value(idx))
          };
          tx.send(op).unwrap();
        }
        r.recv().unwrap();
      });
    });
  group.finish();

  drop(tx);
  threads.into_iter().for_each(|t| t.join().unwrap());
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
