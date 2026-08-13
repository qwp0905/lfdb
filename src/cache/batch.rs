use std::{
  marker::PhantomData,
  mem::ManuallyDrop,
  ptr::NonNull,
  sync::atomic::{AtomicBool, Ordering},
};

use crossbeam::queue::SegQueue;

const MAX_BATCH_SIZE: usize = 32;

pub struct BatchFn<T, F>(ManuallyDrop<F>, PhantomData<fn(&mut T)>);
impl<T, F> BatchFn<T, F> {
  pub const fn new(f: F) -> Self {
    Self(ManuallyDrop::new(f), PhantomData)
  }
  pub const fn task(&mut self) -> BatchTask<T>
  where
    F: FnOnce(&mut T),
  {
    BatchTask::new(NonNull::from_mut(self))
  }
  unsafe fn take(&mut self) -> F {
    unsafe { ManuallyDrop::take(&mut self.0) }
  }
}
pub struct BatchTask<T> {
  ptr: NonNull<()>,
  call: unsafe fn(NonNull<()>, &mut T),
}
impl<T> BatchTask<T> {
  const fn new<F>(ptr: NonNull<BatchFn<T, F>>) -> Self
  where
    F: FnOnce(&mut T),
  {
    Self {
      ptr: ptr.cast(),
      call: call::<T, F>,
    }
  }
  fn call_with(self, data: &mut T) {
    unsafe { (self.call)(self.ptr, data) };
  }
}
unsafe fn call<T, F>(ptr: NonNull<()>, data: &mut T)
where
  F: FnOnce(&mut T),
{
  let f = unsafe { ptr.cast::<BatchFn<T, F>>().as_mut().take() };
  f(data);
}

/**
 * Per-block mutation batch coordinator.
 *
 * This is the same winner/occupied pattern used by the disk task publisher,
 * adapted for cached block mutation. Callers register mutation closures, and
 * one winner owns the batch pass that applies queued handlers to the same
 * `RefedSlot`.
 */
pub struct BatchHandle<T> {
  queue: SegQueue<BatchTask<T>>,
  occupied: AtomicBool,
}
impl<T> BatchHandle<T> {
  pub fn new() -> Self {
    Self {
      queue: SegQueue::new(),
      occupied: AtomicBool::new(false),
    }
  }

  pub fn register(&self, handler: BatchTask<T>) -> bool {
    self.queue.push(handler);
    !self.occupied.fetch_or(true, Ordering::Release)
  }

  /**
   * flush handles with given slot.
   * The lifetime of the batch function which serves as the parent for the registered tasks must be guaranteed.
   */
  pub unsafe fn flush_with(&self, data: &mut T) {
    for handle in (0..MAX_BATCH_SIZE).map_while(|_| self.queue.pop()) {
      handle.call_with(data);
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
unsafe impl<T: Send> Send for BatchHandle<T> {}
unsafe impl<T: Sync> Sync for BatchHandle<T> {}
