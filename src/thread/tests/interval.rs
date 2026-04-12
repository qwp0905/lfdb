use crate::Error;

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
fn test_panic_handling() {
  let work = SingleFn::new(|x: Option<i32>| {
    if let Some(x) = x {
      if x < 0 {
        panic!("Cannot process negative numbers");
      }
      return x * 2;
    }
    0
  });

  let thread = IntervalWorkThread::new(
    "test-panic",
    DEFAULT_STACK_SIZE,
    Duration::from_secs(100),
    work,
  );

  // Normal case
  let result = thread.execute(10).wait();
  assert!(result.is_ok());
  assert_eq!(result.unwrap(), 20);

  // Panic-inducing case
  let result = thread.execute(-5).wait();
  assert!(result.is_err());
  if let Err(Error::Panic(_)) = result {
    // Panic was converted to Error::Panic as expected
  } else {
    panic!("Panic was not converted to Error::Panic");
  }

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
