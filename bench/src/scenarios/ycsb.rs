use std::sync::{
  Arc,
  atomic::{AtomicUsize, Ordering},
};

use criterion::{BenchmarkGroup, Throughput, measurement::WallTime};
use crossbeam::channel::{Sender, unbounded};
use rand::Rng;
use rand_distr::{Distribution, Zipf};

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

fn pre_load<E: BenchmarkDB>(engine: E, count: usize) {
  engine.ensure_table(TABLE);
  let kvs = (0..count).map(|i| (make_key(i), make_value(i)));
  engine.bulk(TABLE, kvs.collect());
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

fn spawn_workers(
  engine: Arc<dyn BenchmarkDB + Send + Sync>,
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
          match op {
            Op::Get(k) => e.get(TABLE, &k),
            Op::Insert(k, v) => e.insert(TABLE, k, v),
            Op::Scan(start, end) => e.scan(TABLE, &start, &end),
            Op::ReadModifyWrite(k, v) => e.read_modify_write(TABLE, k, v),
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

pub fn workload_a<F, E>(
  record_count: usize,
  op_count: usize,
  thread_count: usize,
  mut group: BenchmarkGroup<WallTime>,
  new: F,
) where
  E: BenchmarkDB + Send + Sync + 'static,
  F: Fn() -> E,
{
  {
    let engine = new();
    pre_load(engine, record_count);
  }

  let engine = Arc::new(new());
  let (t, r) = unbounded();
  let (tx, counter, threads) = spawn_workers(engine.clone(), thread_count, &t);

  let zipf = Zipf::new(record_count as u64, ZIPF_EXPONENT).unwrap();
  let rng = &mut rand::thread_rng();

  group
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

pub fn workload_b<E, F>(
  record_count: usize,
  op_count: usize,
  thread_count: usize,
  mut group: BenchmarkGroup<WallTime>,
  new: F,
) where
  E: BenchmarkDB + Send + Sync + 'static,
  F: Fn() -> E,
{
  {
    let engine = new();
    pre_load(engine, record_count);
  }

  let engine = Arc::new(new());
  let (t, r) = unbounded();
  let (tx, counter, threads) = spawn_workers(engine.clone(), thread_count, &t);

  let zipf = Zipf::new(record_count as u64, ZIPF_EXPONENT).unwrap();
  let rng = &mut rand::thread_rng();

  group
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

pub fn workload_d<E, F>(
  record_count: usize,
  op_count: usize,
  thread_count: usize,
  mut group: BenchmarkGroup<WallTime>,
  new: F,
) where
  E: BenchmarkDB + Send + Sync + 'static,
  F: Fn() -> E,
{
  {
    let engine = new();
    pre_load(engine, record_count);
  }

  let engine = Arc::new(new());
  let (t, r) = unbounded();
  let (tx, counter, threads) = spawn_workers(engine.clone(), thread_count, &t);

  let rng = &mut rand::thread_rng();
  let insert_counter = AtomicUsize::new(record_count);

  group
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

pub fn workload_e<E, F>(
  record_count: usize,
  op_count: usize,
  thread_count: usize,
  mut group: BenchmarkGroup<WallTime>,
  new: F,
) where
  E: BenchmarkDB + Send + Sync + 'static,
  F: Fn() -> E,
{
  {
    let engine = new();
    pre_load(engine, record_count);
  }

  let engine = Arc::new(new());
  let (t, r) = unbounded();
  let (tx, counter, threads) = spawn_workers(engine.clone(), thread_count, &t);

  let rng = &mut rand::thread_rng();
  let insert_counter = AtomicUsize::new(record_count);

  group
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

pub fn workload_f<E, F>(
  record_count: usize,
  op_count: usize,
  thread_count: usize,
  mut group: BenchmarkGroup<WallTime>,
  new: F,
) where
  E: BenchmarkDB + Send + Sync + 'static,
  F: Fn() -> E,
{
  {
    let engine = new();
    pre_load(engine, record_count);
  }

  let engine = Arc::new(new());
  let (t, r) = unbounded();
  let (tx, counter, threads) = spawn_workers(engine.clone(), thread_count, &t);

  let zipf = Zipf::new(record_count as u64, ZIPF_EXPONENT).unwrap();
  let rng = &mut rand::thread_rng();

  group
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
