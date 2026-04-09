use super::*;
use crate::utils::ToArc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const DEFAULT_STACK_SIZE: usize = 64 << 10;

fn make() -> (Arc<Spawner>, TimerThread) {
  let spawner = Spawner::new(DEFAULT_STACK_SIZE, 4).to_arc();
  let timer = TimerThread::new(spawner.clone());
  (spawner, timer)
}

#[test]
fn test_basic_register() {
  let (spawner, timer) = make();

  let handle = timer.register(|| 42usize, Duration::from_millis(20));
  assert_eq!(handle.wait().unwrap(), 42);

  timer.close();
  spawner.close();
}

#[test]
fn test_fires_after_delay() {
  let (spawner, timer) = make();

  let start = Instant::now();
  let handle = timer.register(|| Instant::now(), Duration::from_millis(50));
  let fired_at = handle.wait().unwrap();
  let elapsed = fired_at.duration_since(start);

  assert!(
    elapsed >= Duration::from_millis(45),
    "fired too early: {:?}",
    elapsed
  );
  assert!(
    elapsed < Duration::from_millis(500),
    "fired too late: {:?}",
    elapsed
  );

  timer.close();
  spawner.close();
}

#[test]
fn test_ordering() {
  let (spawner, timer) = make();

  let order = AtomicUsize::new(0).to_arc();

  let mk = |delay_ms: u64, expected: usize| {
    let order = order.clone();
    timer.register(
      move || {
        let pos = order.fetch_add(1, Ordering::AcqRel);
        assert_eq!(pos, expected, "out-of-order: delay={}ms", delay_ms);
      },
      Duration::from_millis(delay_ms),
    )
  };

  let h_long = mk(200, 2);
  let h_mid = mk(100, 1);
  let h_short = mk(30, 0);

  h_short.wait().unwrap();
  h_mid.wait().unwrap();
  h_long.wait().unwrap();

  assert_eq!(order.load(Ordering::Acquire), 3);

  timer.close();
  spawner.close();
}

#[test]
fn test_many_tasks() {
  let (spawner, timer) = make();

  let counter = AtomicUsize::new(0).to_arc();
  let n: usize = 100;

  let handles: Vec<_> = (0..n)
    .map(|i| {
      let counter = counter.clone();
      let delay = Duration::from_millis(10 + (i as u64 % 50));
      timer.register(move || counter.fetch_add(1, Ordering::AcqRel), delay)
    })
    .collect();

  for h in handles {
    h.wait().unwrap();
  }

  assert_eq!(counter.load(Ordering::Acquire), n);

  timer.close();
  spawner.close();
}

#[test]
fn test_zero_delay_runs_immediately() {
  let (spawner, timer) = make();

  let start = Instant::now();
  let handle = timer.register(|| Instant::now(), Duration::from_millis(0));
  let fired_at = handle.wait().unwrap();
  let elapsed = fired_at.duration_since(start);

  assert!(
    elapsed < Duration::from_millis(50),
    "zero-delay was not immediate: {:?}",
    elapsed
  );

  timer.close();
  spawner.close();
}

#[test]
fn test_multiple_close() {
  let (spawner, timer) = make();
  timer.close();
  timer.close();
  timer.close();
  spawner.close();
}
