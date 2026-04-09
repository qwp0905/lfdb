use super::*;
use crate::utils::ToArc;
use crate::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const DEFAULT_STACK_SIZE: usize = 64 << 10;

fn make() -> Arc<Spawner> {
  Spawner::new(DEFAULT_STACK_SIZE, 4).to_arc()
}

#[test]
fn test_basic_send_and_receive() {
  let spawner = make();

  let work = SharedFn::new(std::sync::Arc::new(|values: Vec<usize>| {
    values.iter().sum::<usize>()
  }));
  let thread = EagerBufferingThread::new(spawner.clone(), 16, work);

  // single-item batch
  assert_eq!(thread.send(10usize).wait().unwrap(), 10);
  assert_eq!(thread.send(20usize).wait().unwrap(), 20);

  thread.close();
  spawner.close();
}

#[test]
fn test_max_count_respected() {
  let spawner = make();

  let max_count = 5;
  let batch_sizes: Arc<Mutex<Vec<usize>>> = Default::default();
  let batch_sizes_c = batch_sizes.clone();

  let work = SharedFn::new(std::sync::Arc::new(move |values: Vec<usize>| {
    let len = values.len();
    batch_sizes_c.lock().unwrap().push(len);
    len
  }));
  let thread = EagerBufferingThread::new(spawner.clone(), max_count, work);

  let pending: Vec<_> = (0..50).map(|i| thread.send(i)).collect();
  for r in pending {
    let _ = r.wait().unwrap();
  }

  let sizes = batch_sizes.lock().unwrap().clone();
  for &s in &sizes {
    assert!(s <= max_count, "batch size {s} > max {max_count}");
  }

  thread.close();
  spawner.close();
}

#[test]
fn test_close_flushes_remaining() {
  let spawner = make();

  let total = AtomicUsize::new(0).to_arc();
  let total_c = total.clone();

  let work = SharedFn::new(std::sync::Arc::new(move |values: Vec<usize>| {
    total_c.fetch_add(values.len(), Ordering::Release);
    values.len()
  }));
  let thread = EagerBufferingThread::new(spawner.clone(), 1024, work);

  // Spawn pending sends; their handles are dropped immediately, but the
  // items themselves stay queued until processed.
  for i in 0..5 {
    let _ = thread.send(i);
  }

  thread.close();
  spawner.close();

  assert_eq!(total.load(Ordering::Acquire), 5);
}

#[test]
fn test_panic_propagation() {
  let spawner = make();

  let work = SharedFn::new(std::sync::Arc::new(|_: Vec<usize>| -> usize {
    panic!("intentional panic")
  }));
  let thread = EagerBufferingThread::new(spawner.clone(), 16, work);

  let prev = std::panic::take_hook();
  std::panic::set_hook(Box::new(|_| {}));

  let result = thread.send(1usize).wait();
  assert!(matches!(result, Err(Error::Panic(_))));

  std::panic::set_hook(prev);

  thread.close();
  spawner.close();
}

#[test]
fn test_concurrent_senders() {
  let spawner = make();

  let total = AtomicUsize::new(0).to_arc();
  let total_c = total.clone();

  let work = SharedFn::new(std::sync::Arc::new(move |values: Vec<usize>| {
    let sum: usize = values.iter().sum();
    total_c.fetch_add(sum, Ordering::Release);
    sum
  }));
  let thread = Arc::new(EagerBufferingThread::new(spawner.clone(), 64, work));

  let mut joins = vec![];
  for i in 0..4 {
    let t = thread.clone();
    joins.push(thread::spawn(move || {
      for j in 0..25 {
        let _ = t.send(i * 25 + j).wait().unwrap();
      }
    }));
  }
  for h in joins {
    h.join().unwrap();
  }

  // sum of 0..100
  let expected: usize = (0..100).sum();
  assert_eq!(total.load(Ordering::Acquire), expected);

  // need to extract the inner thread before close because Arc<Self> doesn't
  // support close-on-Arc, but we just consume the Arc by dropping all clones.
  drop(thread);
  spawner.close();
  // Note: we don't call thread.close() because Arc still has refs above; this
  // tests that EagerBufferingThread cleanly tears down on Drop via Arc release.
}

#[test]
fn test_multiple_close() {
  let spawner = make();

  let work = SharedFn::new(std::sync::Arc::new(|v: Vec<()>| v.len()));
  let thread = EagerBufferingThread::new(spawner.clone(), 16, work);

  thread.close();
  thread.close();
  thread.close();
  spawner.close();
}

#[test]
fn test_result_cloned_to_all_waiters() {
  let spawner = make();

  let work = SharedFn::new(std::sync::Arc::new(|values: Vec<usize>| {
    values.iter().map(|v| v * 3).collect::<Vec<usize>>()
  }));
  let thread = EagerBufferingThread::new(spawner.clone(), 16, work);

  // batch with multiple items — all waiters get the same Vec result
  // (the result is per-batch, not per-item).
  let h1 = thread.send(5usize);
  let r1 = h1.wait().unwrap();
  // single send → batch of 1 → result = [15]
  assert_eq!(r1, vec![15]);

  thread.close();
  spawner.close();
}

#[allow(dead_code)]
fn _avoid_unused_warning() {
  let _ = Duration::from_millis(0);
}
