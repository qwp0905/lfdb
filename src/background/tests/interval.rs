use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const DEFAULT_STACK_SIZE: usize = 4 << 10;

#[test]
fn test_shared_work_thread_with_timeout() {
  let counter = Arc::new(AtomicUsize::new(0));
  let counter_clone = counter.clone();

  let work = SingleFn::new(move |x: Option<usize>| {
    if x.is_none() {
      counter_clone.store(1, Ordering::Release);
    }
  });
  let thread = IntervalWorkThread::new(
    "test-timeout",
    DEFAULT_STACK_SIZE,
    Duration::from_millis(100),
    work,
  );

  // Send a task
  thread.execute(5).wait().unwrap();

  // Wait a bit to trigger timeout
  thread::sleep(Duration::from_millis(1000));

  // Send another task
  thread.execute(7).wait().unwrap();

  // Check final counter value
  // timeout should called
  assert_eq!(counter.load(Ordering::Acquire), 1);

  thread.close();
}

#[test]
fn test_multiple_close() {
  let work = SingleFn::new(|_: Option<()>| {});
  let thread = IntervalWorkThread::new(
    "test-multiple-close",
    DEFAULT_STACK_SIZE,
    Duration::from_secs(10),
    work,
  );

  thread.close();
  thread.close();
}
