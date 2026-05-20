use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam::queue::SegQueue;

/**
 * Double-buffered queue. switch() atomically flips the active queue and returns
 * the previous one for draining, so producers never block the consumer.
 */
pub struct DoubleBuffer<T> {
  active: AtomicBool,
  queues: [SegQueue<T>; 2],
}
impl<T> DoubleBuffer<T> {
  pub const fn new() -> Self {
    Self {
      active: AtomicBool::new(false),
      queues: [SegQueue::new(), SegQueue::new()],
    }
  }

  pub fn push(&self, value: T) {
    self.queues[self.active.load(Ordering::Acquire) as usize].push(value);
  }

  pub fn switch(&self) -> &SegQueue<T> {
    let i = self.active.fetch_not(Ordering::Release) as usize;
    &self.queues[i]
  }
}
