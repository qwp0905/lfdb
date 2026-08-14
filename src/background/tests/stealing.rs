use super::super::ThreadBuilder;
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const DEFAULT_STACK_SIZE: usize = 64 << 10;

#[test]
fn test_no_timeout() {
  let m: Arc<Mutex<usize>> = Default::default();
  let mc = m.clone();
  let c = AtomicUsize::new(0);
  let counter = Arc::new(AtomicUsize::new(0));
  let counter_clone = counter.clone();

  let work = move |x: usize| {
    let cc = c.fetch_add(1, Ordering::Release);
    let mut m = m.lock().unwrap();
    *m = m.max(cc + 1);
    drop(m);
    thread::sleep(Duration::from_millis(100));
    counter_clone.fetch_add(x, Ordering::Release);
    c.fetch_sub(1, Ordering::Release);
    x * 2
  };

  let thread_count = 4;
  let thread = ThreadBuilder::new()
    .name("test-no-timeout")
    .stack_size(DEFAULT_STACK_SIZE)
    .multi(thread_count)
    .stealing(work);

  // Send multiple tasks
  let receivers: Vec<_> = (1..=(thread_count << 1))
    .map(|i| thread.cooperate(i))
    .collect();
  let results = receivers
    .into_iter()
    .map(|receiver| receiver.wait().expect("closed"))
    .collect::<Vec<usize>>();

  assert_eq!(results, vec![2, 4, 6, 8, 10, 12, 14, 16]);
  assert_eq!(counter.load(Ordering::Acquire), 36); // 1+2+3+4+5+6+7+8 = 36
  assert!(*mc.lock().unwrap() >= thread_count);

  thread.close();
}

#[test]
fn test_multiple_threads() {
  let max = Arc::new(Mutex::new(0));
  let count = AtomicUsize::new(0);
  let max_c = max.clone();

  let work = move |_| {
    let c = count.fetch_add(1, Ordering::Release);
    let mut m = max.lock().unwrap();
    *m = m.max(c + 1);
    drop(m);
    thread::sleep(Duration::from_millis(10));
    count.fetch_sub(1, Ordering::Release);
  };

  let thread_count = 4;
  let thread = ThreadBuilder::new()
    .name("test-multi")
    .stack_size(DEFAULT_STACK_SIZE)
    .multi(thread_count)
    .stealing(work);

  let receivers: Vec<_> = (0..(thread_count << 1))
    .map(|i| thread.cooperate(i))
    .collect();

  // Collect all results
  for receiver in receivers.into_iter() {
    receiver.wait().unwrap();
  }

  assert!(*max_c.lock().unwrap() >= thread_count);

  thread.close();
}

#[test]
fn test_multiple_close() {
  let thread_count = 4;
  let work = |_: ()| {};
  let thread = StealingWorkThread::new(
    "test-multi-close",
    DEFAULT_STACK_SIZE,
    thread_count,
    SharedFn::new(Arc::new(work)),
  );

  thread.close();
  thread.close();
  thread.close();
}
