use std::{
  marker::PhantomData,
  mem::{ManuallyDrop, MaybeUninit},
  pin::Pin,
  ptr::NonNull,
  sync::atomic::{AtomicBool, Ordering},
};

use crossbeam::queue::SegQueue;

use crate::background::{OneshotFulfill, VObject, VPtr as VPtrRaw};

const MAX_BATCH_SIZE: usize = 32;

type VPtr = VPtrRaw<VTable>;

struct VTable {
  call: unsafe fn(NonNull<()>, NonNull<()>),
  complete: unsafe fn(NonNull<()>),
}
unsafe fn call<T, R, F>(ptr: NonNull<()>, data: NonNull<()>)
where
  F: FnOnce(&mut T) -> R + Send,
{
  let task = VPtr::get_mut::<BatchPayload<T, R, F>>(ptr);
  task.call(data.cast().as_mut());
}
unsafe fn complete<T, R, F>(ptr: NonNull<()>)
where
  F: FnOnce(&mut T) -> R + Send,
{
  let task = VPtr::get_mut::<BatchPayload<T, R, F>>(ptr);
  task.complete();
}

struct BatchPayload<T, R, F> {
  handler: ManuallyDrop<F>,
  result: MaybeUninit<R>,
  fulfiller: ManuallyDrop<OneshotFulfill<R>>,
  _marker: PhantomData<fn(&mut T)>,
}
impl<T, R, F> BatchPayload<T, R, F> {
  const fn new(handler: F, fulfiller: OneshotFulfill<R>) -> Self {
    Self {
      handler: ManuallyDrop::new(handler),
      result: MaybeUninit::uninit(),
      fulfiller: ManuallyDrop::new(fulfiller),
      _marker: PhantomData,
    }
  }

  unsafe fn call(&mut self, data: &mut T)
  where
    F: FnOnce(&mut T) -> R,
  {
    let handler = unsafe { ManuallyDrop::take(&mut self.handler) };
    self.result.write(handler(data));
  }

  unsafe fn complete(&mut self) {
    let fulfiller = unsafe { ManuallyDrop::take(&mut self.fulfiller) };
    fulfiller.fulfill(self.result.assume_init_read());
  }
}

#[repr(C)]
pub struct BatchFn<T, R, F>(VObject<BatchPayload<T, R, F>, VTable>);
impl<T, R, F> BatchFn<T, R, F>
where
  F: FnOnce(&mut T) -> R + Send,
{
  const VTABLE: VTable = VTable {
    call: call::<T, R, F>,
    complete: complete::<T, R, F>,
  };

  pub const fn new(handler: F, fulfiller: OneshotFulfill<R>) -> Self {
    Self(VObject::new(
      BatchPayload::new(handler, fulfiller),
      &Self::VTABLE,
    ))
  }
  pub fn task(self: Pin<&mut Self>) -> BatchTask<T> {
    // SAFETY: Only take the pinned object's address; no field is moved.
    BatchTask::new(unsafe { self.get_unchecked_mut() }.0.get_ptr())
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
  pub unsafe fn call_with(&mut self, data: &mut T) {
    let vtable = self.ptr.vtable();
    let ptr = self.ptr.erased();
    (vtable.call)(ptr, NonNull::from_mut(data).cast());
  }

  pub unsafe fn complete(self) {
    let vtable = self.ptr.vtable();
    let ptr = self.ptr.erased();
    drop(self); // must drop before call handler because of vobject's lifetime
    (vtable.complete)(ptr);
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

  pub fn drain_tasks(&self) -> impl Iterator<Item = BatchTask<T>> + '_ {
    (0..MAX_BATCH_SIZE).map_while(|_| self.queue.pop())
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
