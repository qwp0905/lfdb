use super::*;

use std::{
  sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc, Arc, Barrier,
  },
  thread,
  time::Duration,
};

#[test]
fn limits_concurrent_entries() {
  const PERMITS: usize = 3;
  const THREADS: usize = 16;
  const ROUNDS: usize = 64;

  let semaphore = Arc::new(Semaphore::new(PERMITS as u32));
  let barrier = Arc::new(Barrier::new(THREADS));
  let active = Arc::new(AtomicUsize::new(0));
  let max_active = Arc::new(AtomicUsize::new(0));
  let entered = Arc::new(AtomicUsize::new(0));

  let handles = (0..THREADS)
    .map(|_| {
      let semaphore = Arc::clone(&semaphore);
      let barrier = Arc::clone(&barrier);
      let active = Arc::clone(&active);
      let max_active = Arc::clone(&max_active);
      let entered = Arc::clone(&entered);

      thread::spawn(move || {
        barrier.wait();

        for _ in 0..ROUNDS {
          let _permit = semaphore.acquire();
          let current = active.fetch_add(1, Ordering::AcqRel) + 1;
          assert!(current <= PERMITS);

          max_active.fetch_max(current, Ordering::AcqRel);
          entered.fetch_add(1, Ordering::Relaxed);

          thread::yield_now();
          active.fetch_sub(1, Ordering::AcqRel);
        }
      })
    })
    .collect::<Vec<_>>();

  for handle in handles {
    handle.join().unwrap();
  }

  assert_eq!(entered.load(Ordering::Acquire), THREADS * ROUNDS);
  assert!(max_active.load(Ordering::Acquire) <= PERMITS);
  assert_eq!(active.load(Ordering::Acquire), 0);
}

#[test]
fn acquire_waits_until_permit_is_released() {
  let semaphore = Arc::new(Semaphore::new(1));
  let permit = semaphore.acquire();
  let (ready_tx, ready_rx) = mpsc::channel();
  let (acquired_tx, acquired_rx) = mpsc::channel();

  let handle = {
    let semaphore = Arc::clone(&semaphore);
    thread::spawn(move || {
      ready_tx.send(()).unwrap();
      let _permit = semaphore.acquire();
      acquired_tx.send(()).unwrap();
    })
  };

  ready_rx.recv().unwrap();
  assert!(acquired_rx.recv_timeout(Duration::from_millis(20)).is_err());

  drop(permit);
  acquired_rx.recv_timeout(Duration::from_secs(1)).unwrap();
  handle.join().unwrap();
}
