use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const DEFAULT_STACK_SIZE: usize = 64 << 10;

#[test]
fn test_basic_spawn() {
  let spawner = Spawner::new(DEFAULT_STACK_SIZE, 2);

  let handle = spawner.spawn(|| 42usize);
  assert_eq!(handle.wait().unwrap(), 42);

  spawner.close();
}

#[test]
fn test_multiple_spawns() {
  let spawner = Spawner::new(DEFAULT_STACK_SIZE, 4);
  let counter = Arc::new(AtomicUsize::new(0));

  let handles: Vec<_> = (1..=10)
    .map(|i| {
      let counter = counter.clone();
      spawner.spawn(move || {
        counter.fetch_add(i, Ordering::Release);
        i * 2
      })
    })
    .collect();

  let results: Vec<usize> = handles.into_iter().map(|h| h.wait().unwrap()).collect();

  assert_eq!(results, vec![2, 4, 6, 8, 10, 12, 14, 16, 18, 20]);
  assert_eq!(counter.load(Ordering::Acquire), 55); // 1+2+...+10

  spawner.close();
}

#[test]
fn test_concurrency_limit() {
  let thread_count = 4;
  let spawner = Spawner::new(DEFAULT_STACK_SIZE, thread_count);

  let max: Arc<Mutex<usize>> = Default::default();
  let in_flight = Arc::new(AtomicUsize::new(0));

  let handles: Vec<_> = (0..(thread_count << 1))
    .map(|_| {
      let max = max.clone();
      let in_flight = in_flight.clone();
      spawner.spawn(move || {
        let cur = in_flight.fetch_add(1, Ordering::Release) + 1;
        {
          let mut m = max.lock().unwrap();
          *m = (*m).max(cur);
        }
        thread::sleep(Duration::from_millis(50));
        in_flight.fetch_sub(1, Ordering::Release);
      })
    })
    .collect();

  for h in handles {
    h.wait().unwrap();
  }

  assert_eq!(*max.lock().unwrap(), thread_count);

  spawner.close();
}

#[test]
fn test_panic_propagation() {
  let spawner = Spawner::new(DEFAULT_STACK_SIZE, 2);

  let prev = std::panic::take_hook();
  std::panic::set_hook(Box::new(|_| {}));

  let handle = spawner.spawn(|| -> usize { panic!("boom") });
  assert!(handle.wait().is_err());

  std::panic::set_hook(prev);

  // spawner remains usable after a task panics
  let ok = spawner.spawn(|| 7usize);
  assert_eq!(ok.wait().unwrap(), 7);

  spawner.close();
}

#[test]
fn test_multiple_close() {
  let spawner = Spawner::new(DEFAULT_STACK_SIZE, 4);
  spawner.spawn(|| ()).wait().unwrap();

  spawner.close();
  spawner.close();
  spawner.close();
}
