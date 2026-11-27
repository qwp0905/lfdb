use std::cell::Cell;

use super::*;

struct Tracked<'a> {
  id: usize,
  drops: &'a [Cell<usize>],
}
impl<'a> Tracked<'a> {
  const fn new(id: usize, drops: &'a [Cell<usize>]) -> Self {
    Self { id, drops }
  }
}
impl Drop for Tracked<'_> {
  fn drop(&mut self) {
    let count = &self.drops[self.id];
    count.set(count.get() + 1);
  }
}

fn counters(len: usize) -> Vec<Cell<usize>> {
  (0..len).map(|_| Cell::new(0)).collect()
}

#[test]
fn test_pop_follows_push_order() {
  for count in [0, 1, 2, 1024] {
    let queue = MsQueue::new();
    for i in 0..count {
      queue.push(i);
    }
    for i in 0..count {
      assert_eq!(queue.pop(), Some(i));
    }
    assert_eq!(queue.pop(), None);
  }
}

#[test]
fn test_push_after_empty() {
  let queue = MsQueue::new();
  assert_eq!(queue.pop(), None);
  for i in 0..1024 {
    queue.push(i);
    queue.push(i + 1);
    assert_eq!(queue.pop(), Some(i));
    assert_eq!(queue.pop(), Some(i + 1));
    assert_eq!(queue.pop(), None);
  }
}

#[test]
fn test_popped_value_is_owned_by_caller() {
  let drops = counters(1);
  let queue = MsQueue::new();
  queue.push(Tracked::new(0, &drops));

  let popped = queue.pop().unwrap();
  drop(queue);
  assert_eq!(drops[0].get(), 0);

  drop(popped);
  assert_eq!(drops[0].get(), 1);
}

#[test]
fn test_drop_releases_each_remaining_value_once() {
  for (push_count, pop_count) in [(0, 0), (1, 0), (1, 1), (3, 1), (64, 17)] {
    let drops = counters(push_count);
    let queue = MsQueue::new();
    for id in 0..push_count {
      queue.push(Tracked::new(id, &drops));
    }

    for id in 0..pop_count {
      let popped = queue.pop().unwrap();
      assert_eq!(popped.id, id);
    }
    for (id, count) in drops.iter().enumerate() {
      assert_eq!(count.get(), usize::from(id < pop_count));
    }

    drop(queue);
    assert!(drops.iter().all(|count| count.get() == 1));
  }
}
