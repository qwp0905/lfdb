use std::{iter::repeat, sync::atomic::Ordering};

use super::{loom::AtomicBool, MpscQueue};

pub struct BatchQueue<T> {
  queue: MpscQueue<T>,
  occupied: AtomicBool,
}
impl<T> BatchQueue<T> {
  pub fn new() -> Self {
    Self {
      queue: MpscQueue::new(),
      occupied: AtomicBool::new(false),
    }
  }

  pub fn push_and_compete(&self, value: T) -> bool {
    self.queue.push(value);
    !self.occupied.fetch_or(true, Ordering::AcqRel)
  }

  pub fn try_release(&self) -> bool {
    self.occupied.fetch_and(false, Ordering::AcqRel);
    if self.queue.is_empty() {
      return true;
    }
    self.occupied.fetch_or(true, Ordering::Acquire)
  }

  pub unsafe fn drain_n(&self, count: usize) -> impl Iterator<Item = T> + '_ {
    (0..count).map_while(|_| unsafe { self.queue.pop() })
  }

  pub unsafe fn drain_all(&self) -> impl Iterator<Item = T> + '_ {
    repeat(()).map_while(|_| unsafe { self.queue.pop() })
  }
}
impl<T> Default for BatchQueue<T> {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(all(test, loom))]
#[path = "tests/batch_loom.rs"]
mod tests;
