
use std::{
  thread::{sleep, spawn},
  time::Duration,
};

use super::*;

#[test]
fn test_fulfill() {
  let (a, b) = oneshot::<usize>();
  let v = 100;

  spawn(move || {
    sleep(Duration::from_millis(100));
    b.fulfill(v);
  });
  assert_eq!(a.wait().expect("should be fulfilled"), v)
}

#[test]
fn test_fulfill_before_wait() {
  let (rx, tx) = oneshot::<usize>();
  tx.fulfill(42);
  assert_eq!(rx.wait().expect("should be fulfilled"), 42);
}

#[test]
fn test_disconnected_on_fulfill_drop() {
  let (rx, tx) = oneshot::<usize>();
  drop(tx);
  assert!(matches!(rx.wait(), Err(Error::ChannelDisconnected)));
}

#[test]
fn test_disconnected_on_fulfill_drop_async() {
  let (rx, tx) = oneshot::<usize>();
  spawn(move || {
    sleep(Duration::from_millis(100));
    drop(tx);
  });
  assert!(matches!(rx.wait(), Err(Error::ChannelDisconnected)));
}

#[test]
fn test_no_leak_on_rx_drop_after_fulfill() {
  use std::sync::atomic::{AtomicUsize, Ordering};

  static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

  struct Tracked;
  impl Drop for Tracked {
    fn drop(&mut self) {
      DROP_COUNT.fetch_add(1, Ordering::SeqCst);
    }
  }

  DROP_COUNT.store(0, Ordering::SeqCst);
  let (rx, tx) = oneshot::<Tracked>();
  tx.fulfill(Tracked);
  drop(rx);
  assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 1);
}

#[test]
fn test_no_leak_on_fulfill_then_wait() {
  use std::sync::atomic::{AtomicUsize, Ordering};

  static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

  struct Tracked;
  impl Drop for Tracked {
    fn drop(&mut self) {
      DROP_COUNT.fetch_add(1, Ordering::SeqCst);
    }
  }

  DROP_COUNT.store(0, Ordering::SeqCst);
  let (rx, tx) = oneshot::<Tracked>();
  tx.fulfill(Tracked);
  let _val = rx.wait().expect("should be fulfilled");
  drop(_val);
  assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 1);
}

#[test]
fn test_no_leak_on_fulfill_after_rx_drop() {
  use std::sync::atomic::{AtomicUsize, Ordering};

  static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

  struct Tracked;
  impl Drop for Tracked {
    fn drop(&mut self) {
      DROP_COUNT.fetch_add(1, Ordering::SeqCst);
    }
  }

  DROP_COUNT.store(0, Ordering::SeqCst);
  let (rx, tx) = oneshot::<Tracked>();
  drop(rx);
  tx.fulfill(Tracked);
  assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 1);
}
