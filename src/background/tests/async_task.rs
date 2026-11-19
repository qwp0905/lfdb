use std::{
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
  thread::{sleep, spawn},
  time::Duration,
};

use super::*;

#[test]
fn test_fulfill() {
  let (tx, rx) = create_task::<usize>();
  let v = 100;

  spawn(move || {
    sleep(Duration::from_millis(100));
    tx.fulfill(v);
  });
  assert_eq!(rx.wait().expect("should be fulfilled"), v)
}

#[test]
fn test_fulfill_before_wait() {
  let (tx, rx) = create_task::<usize>();
  tx.fulfill(42);
  assert_eq!(rx.wait().expect("should be fulfilled"), 42);
}

#[test]
fn test_disconnected_on_fulfill_drop() {
  let (tx, rx) = create_task::<usize>();
  drop(tx);
  assert!(matches!(rx.wait(), Err(WaitDisconnectedError)));
}

#[test]
fn test_disconnected_on_fulfill_drop_async() {
  let (tx, rx) = create_task::<usize>();
  spawn(move || {
    sleep(Duration::from_millis(100));
    drop(tx);
  });
  assert!(matches!(rx.wait(), Err(WaitDisconnectedError)));
}

struct Tracked(Arc<AtomicUsize>);
impl Drop for Tracked {
  fn drop(&mut self) {
    self.0.fetch_add(1, Ordering::SeqCst);
  }
}

#[test]
fn test_no_leak_on_rx_drop_after_fulfill() {
  let dropped = Arc::new(AtomicUsize::new(0));
  let (tx, rx) = create_task::<Tracked>();
  tx.fulfill(Tracked(dropped.clone()));
  drop(rx);
  assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn test_no_leak_on_fulfill_then_wait() {
  let dropped = Arc::new(AtomicUsize::new(0));
  let (tx, rx) = create_task::<Tracked>();
  tx.fulfill(Tracked(dropped.clone()));
  let value = rx.wait().expect("should be fulfilled");
  drop(value);
  assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn test_no_leak_on_fulfill_after_rx_drop() {
  let dropped = Arc::new(AtomicUsize::new(0));
  let (tx, rx) = create_task::<Tracked>();
  drop(rx);
  tx.fulfill(Tracked(dropped.clone()));
  assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn test_function_transforms_input() {
  let (tx, rx) = create_task_with(|v: usize| v.to_string());
  tx.fulfill(7);
  assert_eq!(rx.wait().expect("should be fulfilled"), "7");
}

#[test]
fn test_function_runs_after_rx_drop() {
  let called = Arc::new(AtomicUsize::new(0));
  let (tx, rx) = {
    let called = called.clone();
    create_task_with(move |v: usize| {
      called.fetch_add(v, Ordering::SeqCst);
    })
  };
  drop(rx);
  tx.fulfill(3);
  assert_eq!(called.load(Ordering::SeqCst), 3);
}

#[test]
fn test_unfulfilled_function_is_dropped_without_running() {
  let called = Arc::new(AtomicUsize::new(0));
  let dropped = Arc::new(AtomicUsize::new(0));
  let (tx, rx) = {
    let called = called.clone();
    let captured = Tracked(dropped.clone());
    create_task_with(move |_: ()| {
      let _captured = captured;
      called.fetch_add(1, Ordering::SeqCst);
    })
  };
  drop(tx);
  assert!(matches!(rx.wait(), Err(WaitDisconnectedError)));
  assert_eq!(called.load(Ordering::SeqCst), 0);
  assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn test_try_wait_returns_task_until_fulfilled() {
  let (tx, rx) = create_task::<usize>();
  let Err(TryWaitError::Empty(rx)) = rx.try_wait() else {
    panic!("should be empty before fulfill");
  };
  tx.fulfill(9);
  assert!(matches!(rx.try_wait(), Ok(9)));
}

#[test]
fn test_try_wait_reports_disconnect() {
  let (tx, rx) = create_task::<usize>();
  drop(tx);
  assert!(matches!(rx.try_wait(), Err(TryWaitError::Disconnected)));
}

#[test]
fn test_try_park_reports_pending_until_finished() {
  let (tx, rx) = create_task::<usize>();
  assert!(rx.try_park());
  tx.fulfill(1);
  assert!(!rx.try_park());

  let (tx, rx) = create_task::<usize>();
  drop(tx);
  assert!(!rx.try_park());
}

#[test]
fn test_panicking_function_disconnects_receiver() {
  let (tx, rx) = create_task_with(|_: ()| -> usize { panic!("function failed") });
  let sender = spawn(move || tx.fulfill(()));
  assert!(sender.join().is_err());
  assert!(matches!(rx.wait(), Err(WaitDisconnectedError)));
}
