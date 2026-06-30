use std::{
  mem::ManuallyDrop,
  sync::atomic::{AtomicBool, Ordering},
};

use crossbeam::queue::SegQueue;

use super::RefedSlot;

const MAX_BATCH_SIZE: usize = 32;

pub struct BatchFn<F>(ManuallyDrop<F>);
impl<F> BatchFn<F> {
  pub const fn new(f: F) -> Self {
    Self(ManuallyDrop::new(f))
  }
  pub const fn task(&mut self) -> BatchTask
  where
    F: FnOnce(&mut RefedSlot),
  {
    BatchTask::new(self as *mut _)
  }
}
pub struct BatchTask {
  ptr: *mut (),
  call: unsafe fn(*mut (), &mut RefedSlot),
}
impl BatchTask {
  const fn new<F>(ptr: *mut BatchFn<F>) -> Self
  where
    F: FnOnce(&mut RefedSlot),
  {
    Self {
      ptr: ptr.cast(),
      call: call::<F>,
    }
  }
  fn call_with(self, slot: &mut RefedSlot) {
    unsafe { (self.call)(self.ptr, slot) };
  }
}
unsafe fn call<F>(ptr: *mut (), slot: &mut RefedSlot)
where
  F: FnOnce(&mut RefedSlot),
{
  let f = unsafe { &mut (*ptr.cast::<BatchFn<F>>()).0 };
  let task = unsafe { ManuallyDrop::take(f) };
  task(slot);
}

/**
 * Per-block mutation batch coordinator.
 *
 * This is the same winner/occupied pattern used by the disk task publisher,
 * adapted for cached block mutation. Callers register mutation closures, and
 * one winner owns the batch pass that applies queued handlers to the same
 * `RefedSlot`.
 */
pub struct BatchHandle {
  queue: SegQueue<BatchTask>,
  occupied: AtomicBool,
}
impl BatchHandle {
  pub fn new() -> Self {
    Self {
      queue: SegQueue::new(),
      occupied: AtomicBool::new(false),
    }
  }

  pub fn register(&self, handler: BatchTask) -> bool {
    self.queue.push(handler);
    !self.occupied.fetch_or(true, Ordering::Release)
  }

  /**
   * flush handles with given slot.
   * The lifetime of the batch function which serves as the parent for the registered tasks must be guaranteed.
   */
  pub unsafe fn flush_with(&self, slot: &mut RefedSlot) {
    for handle in (0..MAX_BATCH_SIZE).map_while(|_| self.queue.pop()) {
      handle.call_with(slot);
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
