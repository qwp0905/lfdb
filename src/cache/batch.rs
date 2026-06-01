use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam::queue::SegQueue;

use super::RefedSlot;
use crate::{
  objects::TypedObject,
  thread::{oneshot, Oneshot, OneshotFulfill},
  Result,
};

const MAX_BATCH_SIZE: usize = 32;

pub type BatchHandler<'a> = dyn FnOnce(&mut RefedSlot, &mut TypedObject) -> Result + 'a;
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

  pub fn flush_with(&self, slot: &mut RefedSlot, obj: &mut TypedObject) {
    for (handle, f) in (0..MAX_BATCH_SIZE).map_while(|_| self.queue.pop()) {
      f.fulfill(handle(slot, obj));
    }
  }

  pub fn try_release(&self) -> bool {
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
