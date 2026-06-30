use std::{
  cell::UnsafeCell,
  mem::{ManuallyDrop, MaybeUninit},
  sync::atomic::{AtomicBool, Ordering},
  thread::{current, park, yield_now, Thread},
};

use crossbeam::queue::SegQueue;

use super::RefedSlot;

const MAX_BATCH_SIZE: usize = 32;

pub struct BatchTask {
  ptr: *mut (),
  call: unsafe fn(*mut (), &mut RefedSlot),
}

impl BatchTask {
  const fn new<F, R>(job: *mut BatchJob<F, R>) -> Self
  where
    F: FnOnce(&mut RefedSlot) -> R,
  {
    Self {
      ptr: job.cast(),
      call: call::<F, R>,
    }
  }
  fn call(self, slot: &mut RefedSlot) {
    unsafe { (self.call)(self.ptr, slot) };
  }
}

unsafe fn call<F, R>(ptr: *mut (), slot: &mut RefedSlot)
where
  F: FnOnce(&mut RefedSlot) -> R,
{
  let job = &mut *ptr.cast::<BatchJob<F, R>>();
  let handler = ManuallyDrop::take(job.handler.get_mut());

  (*job.result.get()).write(handler(slot));
  job.done.store(true, Ordering::Release);
  job.caller.unpark();
}

pub struct BatchJob<F, R> {
  handler: UnsafeCell<ManuallyDrop<F>>,
  result: UnsafeCell<MaybeUninit<R>>,
  done: AtomicBool,
  caller: Thread,
}
impl<F, R> BatchJob<F, R> {
  pub fn new(task: F) -> Self {
    Self {
      handler: UnsafeCell::new(ManuallyDrop::new(task)),
      result: UnsafeCell::new(MaybeUninit::uninit()),
      done: AtomicBool::new(false),
      caller: current(),
    }
  }
  pub fn get_task(&mut self) -> BatchTask
  where
    F: FnOnce(&mut RefedSlot) -> R,
  {
    BatchTask::new(self as *mut _)
  }

  pub fn wait(&self) -> R {
    let mut backoff = 0;
    loop {
      if self.done.load(Ordering::Acquire) {
        return unsafe { (*self.result.get()).assume_init_read() };
      }

      if backoff < MAX_YIELD {
        yield_now();
        backoff += 1;
      } else {
        park();
        backoff = 0;
      }
    }
  }
}

const MAX_YIELD: u8 = 10;

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

  pub fn flush_with(&self, slot: &mut RefedSlot) {
    for task in (0..MAX_BATCH_SIZE).map_while(|_| self.queue.pop()) {
      task.call(slot);
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
