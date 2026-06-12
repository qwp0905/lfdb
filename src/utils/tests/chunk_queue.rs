use std::{
  mem::forget,
  sync::atomic::{AtomicUsize, Ordering},
};

use super::*;

#[test]
fn test_push_and_pop() {
  let mut queue = ChunkQueue::new();
  let count = 1024;
  for i in 0..count {
    assert_eq!(queue.len(), i);
    queue.push(i);
  }

  assert_eq!(queue.len(), count);

  for i in 0..count {
    assert_eq!(queue.len(), count - i);
    assert_eq!(queue.pop(), Some(i));
  }

  assert!(queue.is_empty());
  assert_eq!(queue.pop(), None);
  assert_eq!(queue.pop(), None);
  assert_eq!(queue.pop(), None);
}

#[test]
fn test_drop() {
  static COUNTER: AtomicUsize = AtomicUsize::new(0);

  struct DC;
  impl Drop for DC {
    fn drop(&mut self) {
      COUNTER.fetch_add(1, Ordering::Relaxed);
    }
  }

  let push_count = 65536;
  let pop_count = 1297;
  {
    let mut queue = ChunkQueue::new();
    for _ in 0..push_count {
      queue.push(DC);
    }

    for _ in 0..pop_count {
      if let Some(v) = queue.pop() {
        forget(v);
      }
    }
  }

  assert_eq!(COUNTER.load(Ordering::Relaxed), push_count - pop_count);
}
