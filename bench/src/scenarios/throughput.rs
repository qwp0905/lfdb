use std::sync::Arc;

use criterion::{BenchmarkGroup, Throughput, measurement::WallTime};
use crossbeam::channel::{Sender, unbounded};

use crate::BenchmarkDB;

const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 256;
const SEQ_SIZE: usize = 1_000;
const CONC_SIZE: usize = 10_000;
const CONC_THREADS: usize = 128;
const TABLE: &str = "bench";

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

fn pre_load<E: BenchmarkDB>(engine: &E, count: usize) {
  engine.ensure_table(TABLE);
  let kvs = (0..count).map(|i| (make_key(i), make_value(i)));
  engine.bulk(TABLE, kvs.collect());
}

pub fn sequential_get<E, F>(mut group: BenchmarkGroup<WallTime>, new: F)
where
  E: BenchmarkDB + Send + Sync + 'static,
  F: Fn() -> E,
{
  let keys: Vec<_> = (0..SEQ_SIZE).map(make_key).collect();

  let engine = new();
  pre_load(&engine, SEQ_SIZE);

  group
    .throughput(Throughput::Elements(SEQ_SIZE as u64))
    .bench_function("bench", |b| {
      b.iter(|| {
        for i in 0..SEQ_SIZE {
          engine.get(TABLE, &keys[i]);
        }
      });
    });
  group.finish();
}

pub fn sequential_insert<E, F>(mut group: BenchmarkGroup<WallTime>, new: F)
where
  E: BenchmarkDB + Send + Sync + 'static,
  F: Fn() -> E,
{
  let keys: Vec<_> = (0..SEQ_SIZE).map(make_key).collect();
  let values: Vec<_> = (0..SEQ_SIZE).map(make_value).collect();

  let engine = new();

  group
    .throughput(Throughput::Elements(SEQ_SIZE as u64))
    .bench_function("bench", |b| {
      b.iter(|| {
        engine.ensure_table(TABLE);
        for i in 0..SEQ_SIZE {
          engine.insert(TABLE, keys[i].clone(), values[i].clone());
        }
        engine.drop_table(TABLE);
      });
    });
  group.finish();
}

pub fn sequential_update<E, F>(mut group: BenchmarkGroup<WallTime>, new: F)
where
  E: BenchmarkDB + Send + Sync + 'static,
  F: Fn() -> E,
{
  let keys: Vec<_> = (0..SEQ_SIZE).map(make_key).collect();
  let values: Vec<_> = (0..SEQ_SIZE).map(make_value).collect();

  let engine = new();
  pre_load(&engine, SEQ_SIZE);

  group
    .throughput(Throughput::Elements(SEQ_SIZE as u64))
    .bench_function("bench", |b| {
      b.iter(|| {
        for i in 0..SEQ_SIZE {
          engine.insert(TABLE, keys[i].clone(), values[i].clone());
        }
      });
    });
}

pub fn concurrent_get<E, F>(mut group: BenchmarkGroup<WallTime>, new: F)
where
  E: BenchmarkDB + Send + Sync + 'static,
  F: Fn() -> E,
{
  let keys: Vec<_> = (0..CONC_SIZE).map(make_key).collect();

  let engine = Arc::new(new());
  pre_load(std::ops::Deref::deref(&engine), CONC_SIZE);

  let (tx, rx) = unbounded::<(Vec<u8>, Sender<()>)>();
  let threads: Vec<_> = (0..CONC_THREADS)
    .map(|_| {
      let rx = rx.clone();
      let e = engine.clone();
      std::thread::spawn(move || {
        while let Ok((k, done)) = rx.recv() {
          e.get(TABLE, &k);
          done.send(()).unwrap();
        }
      })
    })
    .collect();

  group
    .throughput(Throughput::Elements(SEQ_SIZE as u64))
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

pub fn concurrent_insert<E, F>(mut group: BenchmarkGroup<WallTime>, new: F)
where
  E: BenchmarkDB + Send + Sync + 'static,
  F: Fn() -> E,
{
  let keys: Vec<_> = (0..CONC_SIZE).map(make_key).collect();
  let values: Vec<_> = (0..CONC_SIZE).map(make_value).collect();

  let engine = Arc::new(new());

  let (tx, rx) = unbounded::<(Vec<u8>, Vec<u8>, Sender<()>)>();
  let threads: Vec<_> = (0..CONC_THREADS)
    .map(|_| {
      let rx = rx.clone();
      let e = engine.clone();
      std::thread::spawn(move || {
        while let Ok((k, v, done)) = rx.recv() {
          e.insert(TABLE, k, v);
          done.send(()).unwrap();
        }
      })
    })
    .collect();

  group
    .throughput(Throughput::Elements(SEQ_SIZE as u64))
    .bench_function("bench", |b| {
      b.iter(|| {
        engine.ensure_table(TABLE);
        let mut waiting = Vec::with_capacity(CONC_SIZE);
        for i in 0..CONC_SIZE {
          let (t, r) = unbounded();
          tx.send((keys[i].clone(), values[i].clone(), t)).unwrap();
          waiting.push(r);
        }
        waiting.into_iter().for_each(|r| r.recv().unwrap());
        engine.drop_table(TABLE);
      });
    });
  group.finish();

  drop(tx);
  threads.into_iter().for_each(|t| t.join().unwrap());
}

pub fn concurrent_update<E, F>(mut group: BenchmarkGroup<WallTime>, new: F)
where
  E: BenchmarkDB + Send + Sync + 'static,
  F: Fn() -> E,
{
  let keys: Vec<_> = (0..CONC_SIZE).map(make_key).collect();
  let values: Vec<_> = (0..CONC_SIZE).map(make_value).collect();

  let engine = Arc::new(new());
  pre_load(std::ops::Deref::deref(&engine), CONC_SIZE);

  let (tx, rx) = unbounded::<(Vec<u8>, Vec<u8>, Sender<()>)>();
  let threads: Vec<_> = (0..CONC_THREADS)
    .map(|_| {
      let rx = rx.clone();
      let e = engine.clone();
      std::thread::spawn(move || {
        while let Ok((k, v, done)) = rx.recv() {
          e.insert(TABLE, k, v);
          done.send(()).unwrap();
        }
      })
    })
    .collect();

  group
    .throughput(Throughput::Elements(SEQ_SIZE as u64))
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
