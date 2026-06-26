use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam::queue::SegQueue;

use super::RefedSlot;
use crate::{
  background::{oneshot, Oneshot, OneshotFulfill},
  Result,
};

const MAX_BATCH_SIZE: usize = 32;

pub type BatchHandler<'a> = dyn FnOnce(&mut RefedSlot) -> Result + 'a;

/**
 * Per-block mutation batch coordinator.
 *
 * This is the same winner/occupied pattern used by the disk task publisher,
 * adapted for cached block mutation. Callers register mutation closures, and
 * one winner owns the batch pass that applies queued handlers to the same
 * `RefedSlot`.
 */
pub struct BatchHandle {
  queue: SegQueue<(Box<BatchHandler<'static>>, OneshotFulfill<Result>)>,
  occupied: AtomicBool,
}
impl BatchHandle {
  pub fn new() -> Self {
    Self {
      queue: SegQueue::new(),
      occupied: AtomicBool::new(false),
    }
  }

  pub fn register(&self, handler: Box<BatchHandler<'static>>) -> (bool, Oneshot<Result>) {
    let (o, f) = oneshot();
    self.queue.push((handler, f));
    (!self.occupied.fetch_or(true, Ordering::Release), o)
  }

  pub fn flush_with(&self, slot: &mut RefedSlot) {
    for (handle, f) in (0..MAX_BATCH_SIZE).map_while(|_| self.queue.pop()) {
      f.fulfill(handle(slot));
    }
  }

  pub fn try_release(&self) -> bool {
    // Same lost-wakeup handoff as the IO task publisher: release ownership, check
    // for newly queued work, and either finish or reacquire ownership to keep
    // draining.
    self.occupied.fetch_and(false, Ordering::Release);
    if self.queue.is_empty() {
      return true;
    }
    if self.occupied.fetch_or(true, Ordering::AcqRel) {
      return true;
    }
    false
  }
}
unsafe impl Send for BatchHandle {}
unsafe impl Sync for BatchHandle {}
