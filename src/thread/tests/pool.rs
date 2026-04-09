use super::*;
use crate::utils::ToArc;
use crate::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const DEFAULT_STACK_SIZE: usize = 64 << 10;

fn make() -> Arc<Spawner> {
  Spawner::new(DEFAULT_STACK_SIZE, 8).to_arc()
}

#[test]
fn test_basic_push_and_wait() {
  let spawner = make();

  let work = SharedFn::new(std::sync::Arc::new(|x: usize| x * 2));
  let pool = SubPool::new(&spawner, work, 4);

  let handles: Vec<_> = (1..=10).map(|i| pool.push(i)).collect();
  let results: Vec<usize> = handles.into_iter().map(|h| h.wait().unwrap()).collect();
  assert_eq!(results, (1..=10).map(|i| i * 2).collect::<Vec<_>>());

  pool.join().unwrap();
  spawner.close();
}

#[test]
fn test_concurrency_bounded_by_count() {
  let spawner = make();

  let count = 3;
  let in_flight = AtomicUsize::new(0).to_arc();
  let max: Arc<Mutex<usize>> = Default::default();
  let in_flight_c = in_flight.clone();
  let max_c = max.clone();

  let work = SharedFn::new(std::sync::Arc::new(move |_: ()| {
    let cur = in_flight_c.fetch_add(1, Ordering::AcqRel) + 1;
    {
      let mut m = max_c.lock().unwrap();
      *m = (*m).max(cur);
    }
    thread::sleep(Duration::from_millis(40));
    in_flight_c.fetch_sub(1, Ordering::AcqRel);
  }));
  let pool = SubPool::new(&spawner, work, count);

  let handles: Vec<_> = (0..(count * 4)).map(|_| pool.push(())).collect();
  for h in handles {
    h.wait().unwrap();
  }

  assert_eq!(*max.lock().unwrap(), count);

  pool.join().unwrap();
  spawner.close();
}

#[test]
fn test_join_drains_remaining() {
  let spawner = make();

  let processed = AtomicUsize::new(0).to_arc();
  let processed_c = processed.clone();

  let work = SharedFn::new(std::sync::Arc::new(move |_: usize| {
    processed_c.fetch_add(1, Ordering::AcqRel);
  }));
  let pool = SubPool::new(&spawner, work, 2);

  // push and immediately drop the handles — items must still be processed
  for i in 0..20 {
    let _ = pool.push(i);
  }

  pool.join().unwrap();
  assert_eq!(processed.load(Ordering::Acquire), 20);

  spawner.close();
}

#[test]
fn test_panic_propagates_to_waiter() {
  let spawner = make();

  let work = SharedFn::new(std::sync::Arc::new(|x: i32| {
    if x < 0 {
      panic!("negative");
    }
    x * 2
  }));
  let pool = SubPool::new(&spawner, work, 2);

  let prev = std::panic::take_hook();
  std::panic::set_hook(Box::new(|_| {}));

  let ok = pool.push(5).wait().unwrap();
  assert_eq!(ok, 10);

  let bad = pool.push(-1).wait();
  assert!(matches!(bad, Err(Error::Panic(_))));

  let again = pool.push(7).wait().unwrap();
  assert_eq!(again, 14);

  std::panic::set_hook(prev);

  pool.join().unwrap();
  spawner.close();
}

#[test]
fn test_lease_lifetime_bound_to_spawner() {
  let spawner = make();
  {
    let work = SharedFn::new(std::sync::Arc::new(|x: usize| x + 1));
    let pool = SubPool::new(&spawner, work, 2);
    let r = pool.push(41).wait().unwrap();
    assert_eq!(r, 42);
    pool.join().unwrap();
  }
  spawner.close();
}
