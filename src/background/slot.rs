use std::{
  cell::UnsafeCell,
  sync::atomic::{AtomicBool, Ordering},
  thread::JoinHandle,
};

/**
 * One-shot storage for a background thread join handle.
 *
 * Background thread handles expose `close` through `&self`, so the join handle
 * must be taken through interior mutability. `ThreadSlot` makes that operation
 * safe at the API level: the first caller to `close` takes the handle, and all
 * later callers observe that the thread has already been closed.
 */
pub struct ThreadSlot<T = ()> {
  closed: AtomicBool,
  handle: UnsafeCell<Option<JoinHandle<T>>>,
}
impl<T> ThreadSlot<T> {
  pub const fn new(handle: JoinHandle<T>) -> Self {
    Self {
      closed: AtomicBool::new(false),
      handle: UnsafeCell::new(Some(handle)),
    }
  }

  pub fn close(&self) -> Option<JoinHandle<T>> {
    // Relaxed is enough here. The atomic RMW is only used as a one-shot
    // gate deciding which caller owns the join handle and is responsible
    // for sending the termination message. It does not publish data.
    if self.closed.fetch_or(true, Ordering::Relaxed) {
      return None;
    }

    // SAFETY: only the caller that observes `closed == false` can reach this
    // point. All later callers return before touching `handle`, so the
    // unsynchronized Option mutation has a single executor.
    unsafe { (*self.handle.get()).take() }
  }
}
unsafe impl<T: Send> Send for ThreadSlot<T> {}
unsafe impl<T: Send + Sync> Sync for ThreadSlot<T> {}
