use std::{
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
  thread::{sleep, spawn},
  time::Duration,
};

use super::*;
use crate::background::{create_task, AsyncTask};

struct Tracked(Arc<AtomicUsize>);
impl Drop for Tracked {
  fn drop(&mut self) {
    self.0.fetch_add(1, Ordering::SeqCst);
  }
}

#[test]
fn test_finished_task_is_selected_and_rest_remain() {
  let (_pending, rx1) = create_task::<usize>();
  let (tx2, rx2) = create_task::<usize>();
  let mut select = Select::new();
  select.push(rx1);
  select.push(rx2);

  tx2.fulfill(2);
  assert_eq!(select.select().expect("should be fulfilled"), 2);
  assert_eq!(select.len(), 1);
}

#[test]
fn test_select_blocks_until_fulfilled() {
  let (_pending, rx1) = create_task::<usize>();
  let (tx2, rx2) = create_task::<usize>();
  let mut select = Select::new();
  select.push(rx1);
  select.push(rx2);

  spawn(move || {
    sleep(Duration::from_millis(100));
    tx2.fulfill(5);
  });
  assert_eq!(select.select().expect("should be fulfilled"), 5);
}

#[test]
fn test_disconnected_task_is_reported() {
  let (tx, rx) = create_task::<usize>();
  let mut select = Select::new();
  select.push(rx);

  drop(tx);
  assert!(matches!(select.select(), Err(WaitDisconnectedError)));
}

#[test]
fn test_empty_select_does_not_block() {
  let mut select = Select::<AsyncTask<usize>>::new();
  assert!(matches!(select.select(), Err(WaitDisconnectedError)));
}

#[test]
fn test_every_result_is_selected_once() {
  const N: usize = 8;
  let mut select = Select::with_capacity(N);
  let mut senders = Vec::with_capacity(N);
  for _ in 0..N {
    let (tx, rx) = create_task::<usize>();
    select.push(rx);
    senders.push(tx);
  }

  spawn(move || {
    for (i, tx) in senders.into_iter().enumerate().rev() {
      sleep(Duration::from_millis(10));
      tx.fulfill(i);
    }
  });

  let mut results = Vec::with_capacity(N);
  while !select.is_empty() {
    results.push(select.select().expect("should be fulfilled"));
  }
  results.sort();
  assert_eq!(results, (0..N).collect::<Vec<_>>());
}

#[test]
fn test_dropped_select_releases_values() {
  let dropped = Arc::new(AtomicUsize::new(0));
  let mut select = Select::new();
  let mut senders = Vec::new();
  for _ in 0..4 {
    let (tx, rx) = create_task::<Tracked>();
    select.push(rx);
    senders.push(tx);
  }

  let late = senders.split_off(2);
  for tx in senders {
    tx.fulfill(Tracked(dropped.clone()));
  }
  drop(select);
  for tx in late {
    tx.fulfill(Tracked(dropped.clone()));
  }
  assert_eq!(dropped.load(Ordering::SeqCst), 4);
}
