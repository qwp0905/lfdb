use std::{
  marker::PhantomData,
  mem::ManuallyDrop,
  ptr::NonNull,
  sync::atomic::{AtomicBool, Ordering},
};

use crossbeam::queue::SegQueue;

use crate::background::{VObject, VPtr as VPtrRaw};

const MAX_BATCH_SIZE: usize = 32;

type VPtr = VPtrRaw<VTable>;

struct VTable {
  call: unsafe fn(NonNull<()>, NonNull<()>),
}
unsafe fn call<T, F>(ptr: NonNull<()>, data: NonNull<()>)
where
  F: FnOnce(&mut T) + Send,
{
  let task = VPtr::get_mut::<BatchPayload<T, F>>(ptr);
  task.call(data.cast().as_mut());
}

struct BatchPayload<T, F> {
  handler: ManuallyDrop<F>,
  _marker: PhantomData<fn(&mut T)>,
}
impl<T, F> BatchPayload<T, F> {
  const fn new(handler: F) -> Self {
    Self {
      handler: ManuallyDrop::new(handler),
      _marker: PhantomData,
    }
  }

  unsafe fn call(&mut self, data: &mut T)
  where
    F: FnOnce(&mut T),
  {
    let handler = unsafe { ManuallyDrop::take(&mut self.handler) };
    handler(data)
  }
}

#[repr(C)]
pub struct BatchFn<T, F>(VObject<BatchPayload<T, F>, VTable>);
impl<T, F> BatchFn<T, F>
where
  F: FnOnce(&mut T) + Send,
{
  const VTABLE: VTable = VTable { call: call::<T, F> };

  pub const fn new(handler: F) -> Self {
    Self(VObject::new(BatchPayload::new(handler), &Self::VTABLE))
  }
  pub const fn task(&mut self) -> BatchTask<T> {
    BatchTask::new(self.0.get_ptr())
  }
}
pub struct BatchTask<T> {
  ptr: VPtr,
  _marker: PhantomData<fn(&mut T)>,
}
impl<T> BatchTask<T> {
  const fn new(ptr: VPtr) -> Self {
    Self {
      ptr,
      _marker: PhantomData,
    }
  }
  unsafe fn call_with(self, data: NonNull<()>) {
    let vtable = self.ptr.vtable();
    let ptr = self.ptr.erased();
    drop(self); // must drop before call handler because of vobject's lifetime
    (vtable.call)(ptr, data);
  }
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
    let ptr = NonNull::from_mut(data).cast();
    for handle in (0..MAX_BATCH_SIZE).map_while(|_| self.queue.pop()) {
      handle.call_with(ptr);
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
