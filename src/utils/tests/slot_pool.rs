use std::sync::{
  atomic::{AtomicUsize, Ordering},
  Arc,
};

use super::*;

#[test]
fn test_new_pool_is_empty() {
  let pool = SlotPool::<usize>::new(4);
  assert_eq!(pool.len(), 0);
  assert!(pool.is_empty());
  assert!(!pool.is_full());
  assert_eq!(pool.capacity(), 4);
}

#[test]
fn test_insert_assigns_indices_in_order() {
  let mut pool = SlotPool::new(4);
  for i in 0..4 {
    assert_eq!(pool.insert(i * 10), Ok(i));
  }
  assert!(pool.is_full());
  for i in 0..4 {
    assert_eq!(pool.get(i), Some(&(i * 10)));
  }
}

#[test]
fn test_insert_into_full_pool_returns_value() {
  let mut pool = SlotPool::new(1);
  assert_eq!(pool.insert(1), Ok(0));
  assert_eq!(pool.insert(2), Err(2));
  assert_eq!(pool.len(), 1);

  let mut empty = SlotPool::new(0);
  assert_eq!(empty.insert(3), Err(3));
}

#[test]
fn test_remove_keeps_other_indices() {
  let mut pool = SlotPool::new(4);
  for i in 0..4 {
    pool.insert(i * 10).unwrap();
  }

  assert_eq!(pool.remove(1), Some(10));
  assert_eq!(pool.len(), 3);
  assert_eq!(pool.get(1), None);
  assert_eq!(pool.get(0), Some(&0));
  assert_eq!(pool.get(2), Some(&20));
  assert_eq!(pool.get(3), Some(&30));
}

#[test]
fn test_last_removed_slot_is_reused_first() {
  let mut pool = SlotPool::new(4);
  for i in 0..4 {
    pool.insert(i).unwrap();
  }

  pool.remove(1).unwrap();
  pool.remove(3).unwrap();
  assert_eq!(pool.insert(100), Ok(3));
  assert_eq!(pool.insert(101), Ok(1));
  assert_eq!(pool.insert(102), Err(102));
}

#[test]
fn test_vacant_or_out_of_range_access_changes_nothing() {
  let mut pool = SlotPool::new(2);
  assert_eq!(pool.remove(0), None);
  assert_eq!(pool.remove(5), None);
  assert_eq!(pool.get(0), None);
  assert_eq!(pool.get(5), None);

  pool.insert(1).unwrap();
  pool.remove(0).unwrap();
  assert_eq!(pool.remove(0), None);
  assert_eq!(pool.len(), 0);

  assert_eq!(pool.insert(2), Ok(0));
  assert_eq!(pool.insert(3), Ok(1));
  assert_eq!(pool.insert(4), Err(4));
}

struct DropCounter(Arc<AtomicUsize>);
impl Drop for DropCounter {
  fn drop(&mut self) {
    self.0.fetch_add(1, Ordering::Relaxed);
  }
}

#[test]
fn test_drop_releases_remaining_values_once() {
  let dropped = Arc::new(AtomicUsize::new(0));
  let mut pool = SlotPool::new(4);
  for _ in 0..4 {
    let _ = pool.insert(DropCounter(dropped.clone()));
  }

  drop(pool.remove(2));
  assert_eq!(dropped.load(Ordering::Relaxed), 1);

  drop(pool);
  assert_eq!(dropped.load(Ordering::Relaxed), 4);
}
