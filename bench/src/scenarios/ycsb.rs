use std::{
  cell::OnceCell,
  mem::ManuallyDrop,
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
  thread::JoinHandle,
};

use criterion::{BenchmarkGroup, SamplingMode, Throughput, measurement::WallTime};
use crossbeam::channel::{Receiver, Sender, unbounded};
use rand::{Rng, seq::SliceRandom};
use rand_distr::{Distribution, Zipf};
use sysinfo::System;

use crate::BenchmarkDB;

const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 256;
const TABLE: &str = "bench";
const ZIPF_EXPONENT: f64 = 0.99;
const SCAN_LENGTH: usize = 100;

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

/**
 * Preloading runs on its own engine instance, so it may use far more cache than
 * the measured instance and finish without evictions. Half of the available
 * memory leaves room for the rest of the host.
 */
fn preload_cache_size() -> usize {
  let mut system = System::new();
  system.refresh_memory();
  (system.available_memory() / 2) as usize
}

const CHUNK: usize = 1000;
fn pre_load<E: BenchmarkDB>(engine: &E, count: usize) {
  engine.ensure_table(TABLE);
  let mut iter = (0..count).map(|i| (make_key(i), make_value(i)));
  loop {
    let chunk = (&mut iter).take(CHUNK).collect::<Vec<_>>();
    if chunk.is_empty() {
      break;
    }
    engine.bulk(TABLE, &chunk);
  }
}

fn zipfian_index(rng: &mut impl Rng, zipf: &Zipf<f64>, record_count: usize) -> usize {
  let sample: f64 = zipf.sample(rng);
  (sample as usize).saturating_sub(1).min(record_count - 1)
}

enum Op {
  Get(Vec<u8>),
  Insert(Vec<u8>, Vec<u8>),
  Scan(Vec<u8>, Vec<u8>),
  ReadModifyWrite(Vec<u8>, Vec<u8>),
}

struct WorkerPool {
  tx: ManuallyDrop<Sender<Op>>,
  pending: Arc<AtomicUsize>,
  done: Receiver<()>,
  threads: Vec<JoinHandle<()>>,
}
impl WorkerPool {
  fn spawn(engine: Arc<dyn BenchmarkDB + Send + Sync>, count: usize) -> Self {
    let (tx, rx) = unbounded::<Op>();
    let (done_tx, done) = unbounded();
    let pending = Arc::new(AtomicUsize::new(0));
    let threads = (0..count)
      .map(|_| {
        let rx = rx.clone();
        let engine = engine.clone();
        let done_tx = done_tx.clone();
        let pending = pending.clone();
        std::thread::Builder::new()
          .name("worker".to_string())
          .spawn(move || {
            while let Ok(op) = rx.recv() {
              match op {
                Op::Get(k) => engine.get(TABLE, &k),
                Op::Insert(k, v) => engine.insert(TABLE, k, v),
                Op::Scan(start, end) => engine.scan(TABLE, &start, &end),
                Op::ReadModifyWrite(k, v) => engine.read_modify_write(TABLE, k, v),
              }
              if pending.fetch_sub(1, Ordering::Release) == 1 {
                done_tx.send(()).unwrap();
              }
            }
          })
          .unwrap()
      })
      .collect();
    Self {
      tx: ManuallyDrop::new(tx),
      pending,
      done,
      threads,
    }
  }

  fn execute(&self, ops: impl ExactSizeIterator<Item = Op>) {
    self.pending.store(ops.len(), Ordering::Release);
    for op in ops {
      self.tx.send(op).unwrap();
    }
    self.done.recv().unwrap();
  }
}
impl Drop for WorkerPool {
  fn drop(&mut self) {
    unsafe { ManuallyDrop::drop(&mut self.tx) };
    self.threads.drain(..).for_each(|t| t.join().unwrap());
  }
}

fn finish<E: BenchmarkDB>(prepared: OnceCell<(Arc<E>, WorkerPool)>) {
  let Some((engine, pool)) = prepared.into_inner() else {
    return;
  };
  drop(pool);
  engine.print_metrics();
}

fn open<E>(dir: &Path, cache_size: usize, thread_count: usize) -> (Arc<E>, WorkerPool)
where
  E: BenchmarkDB + Send + Sync + 'static,
{
  let engine = Arc::new(E::new(cache_size, dir));
  let pool = WorkerPool::spawn(engine.clone(), thread_count);
  (engine, pool)
}

pub fn workload_a<E>(
  dir: &Path,
  cache_size: usize,
  record_count: usize,
  op_count: usize,
  thread_count: usize,
  mut group: BenchmarkGroup<WallTime>,
) where
  E: BenchmarkDB + Send + Sync + 'static,
{
  let prepared = OnceCell::new();
  let zipf = Zipf::new(record_count as f64, ZIPF_EXPONENT).unwrap();
  let rng = &mut rand::rng();
  let mut records = (0..record_count).collect::<Vec<_>>();

  group
    .throughput(Throughput::Elements(op_count as u64))
    .sampling_mode(SamplingMode::Flat)
    .bench_function("50read-50update", |b| {
      let (_, pool) = prepared.get_or_init(|| {
        println!(
          "{record_count} records / {op_count} operations / {thread_count} threads"
        );
        pre_load(&E::new(preload_cache_size(), dir), record_count);
        open::<E>(dir, cache_size, thread_count)
      });
      records.shuffle(rng);
      b.iter(|| {
        pool.execute((0..op_count).map(|_| {
          let idx = records[zipfian_index(rng, &zipf, record_count)];
          let key = make_key(idx);
          if rng.random_bool(0.50) {
            return Op::Get(key);
          }
          Op::Insert(key, make_value(idx))
        }));
      });
    });
  group.finish();
  finish(prepared);
}

pub fn workload_b<E>(
  dir: &Path,
  cache_size: usize,
  record_count: usize,
  op_count: usize,
  thread_count: usize,
  mut group: BenchmarkGroup<WallTime>,
) where
  E: BenchmarkDB + Send + Sync + 'static,
{
  let prepared = OnceCell::new();
  let zipf = Zipf::new(record_count as f64, ZIPF_EXPONENT).unwrap();
  let rng = &mut rand::rng();
  let mut records = (0..record_count).collect::<Vec<_>>();

  group
    .throughput(Throughput::Elements(op_count as u64))
    .sampling_mode(SamplingMode::Flat)
    .bench_function("95read-5update", |b| {
      let (_, pool) = prepared.get_or_init(|| {
        println!(
          "{record_count} records / {op_count} operations / {thread_count} threads"
        );
        pre_load(&E::new(preload_cache_size(), dir), record_count);
        open::<E>(dir, cache_size, thread_count)
      });
      records.shuffle(rng);
      b.iter(|| {
        pool.execute((0..op_count).map(|_| {
          let idx = records[zipfian_index(rng, &zipf, record_count)];
          let key = make_key(idx);
          if rng.random_bool(0.95) {
            return Op::Get(key);
          }
          Op::Insert(key, make_value(idx))
        }));
      });
    });
  group.finish();
  finish(prepared);
}

pub fn workload_d<E>(
  dir: &Path,
  cache_size: usize,
  record_count: usize,
  op_count: usize,
  thread_count: usize,
  mut group: BenchmarkGroup<WallTime>,
) where
  E: BenchmarkDB + Send + Sync + 'static,
{
  let prepared = OnceCell::new();
  let rng = &mut rand::rng();
  let insert_counter = AtomicUsize::new(record_count);
  let zipf = Zipf::new(100.0, 0.5).unwrap();

  group
    .throughput(Throughput::Elements(op_count as u64))
    .sampling_mode(SamplingMode::Flat)
    .bench_function("95read-5insert-latest", |b| {
      let (_, pool) = prepared.get_or_init(|| {
        println!(
          "{record_count} records / {op_count} operations / {thread_count} threads"
        );
        pre_load(&E::new(preload_cache_size(), dir), record_count);
        open::<E>(dir, cache_size, thread_count)
      });
      b.iter(|| {
        pool.execute((0..op_count).map(|_| {
          if rng.random_bool(0.95) {
            let latest = insert_counter.load(Ordering::Relaxed);
            let idx = latest.saturating_sub(zipf.sample(rng) as usize);
            return Op::Get(make_key(idx));
          }
          let idx = insert_counter.fetch_add(1, Ordering::Relaxed);
          Op::Insert(make_key(idx), make_value(idx))
        }));
      });
    });
  group.finish();
  finish(prepared);
}

pub fn workload_e<E>(
  dir: &Path,
  cache_size: usize,
  record_count: usize,
  op_count: usize,
  thread_count: usize,
  mut group: BenchmarkGroup<WallTime>,
) where
  E: BenchmarkDB + Send + Sync + 'static,
{
  fn make_key(id: usize, i: usize) -> Vec<u8> {
    format!("{id:0>3}{i:0>width$}", width = KEY_SIZE - 3)
      .as_bytes()
      .to_vec()
  }

  let prepared = OnceCell::new();
  let rng = &mut rand::rng();
  let mut ids = (0..(u8::MAX as usize)).collect::<Vec<_>>();
  let per_id = record_count / ids.len();
  let insert_counters = (0..ids.len())
    .map(|_| AtomicUsize::new(per_id))
    .collect::<Vec<_>>();
  let key_zipf = Zipf::new(100.0, 0.3).unwrap();
  let id_zipf = Zipf::new(u8::MAX as f64, 0.99).unwrap();

  group
    .throughput(Throughput::Elements(op_count as u64))
    .sampling_mode(SamplingMode::Flat)
    .bench_function("95scan-5insert", |b| {
      let (_, pool) = prepared.get_or_init(|| {
        println!(
          "{record_count} records / {op_count} operations / {thread_count} threads"
        );
        let engine = E::new(preload_cache_size(), dir);
        engine.ensure_table(TABLE);
        for &id in ids.iter() {
          let mut iter = (0..per_id).map(|i| (make_key(id, i), make_value(i)));
          loop {
            let chunk = (&mut iter).take(CHUNK).collect::<Vec<_>>();
            if chunk.is_empty() {
              break;
            }
            engine.bulk(TABLE, &chunk);
          }
        }
        drop(engine);
        open::<E>(dir, cache_size, thread_count)
      });
      ids.shuffle(rng);
      b.iter(|| {
        pool.execute((0..op_count).map(|_| {
          let id = ids[id_zipf.sample(rng) as usize - 1];
          let insert_counter = &insert_counters[id];
          if rng.random_bool(0.95) {
            let idx = insert_counter
              .load(Ordering::Relaxed)
              .saturating_sub(key_zipf.sample(rng) as usize);
            let start = make_key(id, idx.saturating_sub(SCAN_LENGTH));
            let end = make_key(id, idx);
            return Op::Scan(start, end);
          }
          let idx = insert_counter.fetch_add(1, Ordering::Relaxed);
          Op::Insert(make_key(id, idx), make_value(idx))
        }));
      });
    });
  group.finish();
  finish(prepared);
}

pub fn workload_f<E>(
  dir: &Path,
  cache_size: usize,
  record_count: usize,
  op_count: usize,
  thread_count: usize,
  mut group: BenchmarkGroup<WallTime>,
) where
  E: BenchmarkDB + Send + Sync + 'static,
{
  let prepared = OnceCell::new();
  let zipf = Zipf::new(record_count as f64, ZIPF_EXPONENT).unwrap();
  let rng = &mut rand::rng();
  let mut records = (0..record_count).collect::<Vec<_>>();

  group
    .throughput(Throughput::Elements(op_count as u64))
    .sampling_mode(SamplingMode::Flat)
    .bench_function("50read-50rmw", |b| {
      let (_, pool) = prepared.get_or_init(|| {
        println!(
          "{record_count} records / {op_count} operations / {thread_count} threads"
        );
        pre_load(&E::new(preload_cache_size(), dir), record_count);
        open::<E>(dir, cache_size, thread_count)
      });
      records.shuffle(rng);
      b.iter(|| {
        pool.execute((0..op_count).map(|_| {
          let idx = records[zipfian_index(rng, &zipf, record_count)];
          let key = make_key(idx);
          if rng.random_bool(0.50) {
            return Op::Get(key);
          }
          Op::ReadModifyWrite(key, make_value(idx))
        }));
      });
    });
  group.finish();
  finish(prepared);
}
