use super::*;
use crate::utils::ToArc;
use crate::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const DEFAULT_STACK_SIZE: usize = 64 << 10;

fn make() -> (Arc<Spawner>, Arc<TimerThread>) {
  let spawner = Spawner::new(DEFAULT_STACK_SIZE, 8).to_arc();
  let timer = TimerThread::new(spawner.clone()).to_arc();
  (spawner, timer)
}

fn shutdown_runtime(spawner: Arc<Spawner>, timer: Arc<TimerThread>) {
  timer.close();
  spawner.close();
}

#[test]
fn test_count_triggers_flush() {
  let (spawner, timer) = make();

  let max = 5usize;
  let batches: Arc<Mutex<Vec<Vec<usize>>>> = Default::default();
  let batches_c = batches.clone();

  let work = SharedFn::new(std::sync::Arc::new(move |values: Vec<usize>| {
    batches_c.lock().unwrap().push(values.clone());
    values.len()
  }));
  let thread: LazyBufferingThread<usize, usize> = LazyBufferingThread::new(
    timer.clone(),
    spawner.clone(),
    max,
    Duration::from_secs(60),
    work,
  );

  let handles: Vec<_> = (0..max).map(|i| thread.send(i)).collect();
  println!("all send");
  for h in handles {
    let _ = h.wait().unwrap();
  }
  println!("all done");

  let observed = batches.lock().unwrap().clone();
  let total: usize = observed.iter().map(|b| b.len()).sum();
  assert!(
    total >= max,
    "expected >= {} items processed, got {}",
    max,
    total
  );

  thread.close();
  shutdown_runtime(spawner, timer);
}

#[test]
fn test_timeout_triggers_flush() {
  let (spawner, timer) = make();

  let processed = AtomicUsize::new(0).to_arc();
  let processed_c = processed.clone();
  let work = SharedFn::new(std::sync::Arc::new(move |values: Vec<usize>| {
    processed_c.fetch_add(values.len(), Ordering::AcqRel);
    values.len()
  }));

  let thread: LazyBufferingThread<usize, usize> = LazyBufferingThread::new(
    timer.clone(),
    spawner.clone(),
    1024,
    Duration::from_millis(30),
    work,
  );

  let h1 = thread.send(1usize);
  let h2 = thread.send(2usize);
  let _ = h1.wait().unwrap();
  let _ = h2.wait().unwrap();

  assert!(processed.load(Ordering::Acquire) >= 2);

  thread.close();
  shutdown_runtime(spawner, timer);
}

#[test]
fn test_close_drains_pending() {
  let (spawner, timer) = make();

  let processed = AtomicUsize::new(0).to_arc();
  let processed_c = processed.clone();
  let work = SharedFn::new(std::sync::Arc::new(move |values: Vec<usize>| {
    processed_c.fetch_add(values.len(), Ordering::AcqRel);
    values.len()
  }));

  let thread: LazyBufferingThread<usize, usize> = LazyBufferingThread::new(
    timer.clone(),
    spawner.clone(),
    1024,
    Duration::from_secs(60),
    work,
  );

  for i in 0..5 {
    let _ = thread.send(i);
  }

  thread.close();

  assert_eq!(processed.load(Ordering::Acquire), 5);
  shutdown_runtime(spawner, timer);
}

#[test]
fn test_close_when_idle() {
  let (spawner, timer) = make();
  let work = SharedFn::new(std::sync::Arc::new(|_: Vec<usize>| -> usize { 0 }));
  let thread: LazyBufferingThread<usize, usize> = LazyBufferingThread::new(
    timer.clone(),
    spawner.clone(),
    16,
    Duration::from_secs(60),
    work,
  );
  thread.close();
  shutdown_runtime(spawner, timer);
}

#[test]
fn test_panic_propagates_to_waiters() {
  let (spawner, timer) = make();

  let work = SharedFn::new(std::sync::Arc::new(|_: Vec<usize>| -> usize {
    panic!("intentional");
  }));
  let thread: LazyBufferingThread<usize, usize> = LazyBufferingThread::new(
    timer.clone(),
    spawner.clone(),
    2,
    Duration::from_secs(60),
    work,
  );

  let prev = std::panic::take_hook();
  std::panic::set_hook(Box::new(|_| {}));

  let h1 = thread.send(1);
  let h2 = thread.send(2);
  let r1 = h1.wait();
  let r2 = h2.wait();

  assert!(matches!(r1, Err(Error::Panic(_))));
  assert!(matches!(r2, Err(Error::Panic(_))));

  std::panic::set_hook(prev);

  thread.close();
  shutdown_runtime(spawner, timer);
}

#[test]
fn test_multiple_close() {
  let (spawner, timer) = make();
  let work = SharedFn::new(std::sync::Arc::new(|_: Vec<usize>| -> usize { 0 }));
  let thread: LazyBufferingThread<usize, usize> = LazyBufferingThread::new(
    timer.clone(),
    spawner.clone(),
    16,
    Duration::from_secs(60),
    work,
  );
  thread.close();
  thread.close();
  thread.close();
  shutdown_runtime(spawner, timer);
}
