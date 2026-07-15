use std::thread::JoinHandle;

use crossbeam::atomic::AtomicCell;

/**
 * One-shot storage for a background thread join handle.
 *
 * Background thread handles expose `close` through `&self`, so the join handle
 * must be taken through interior mutability. `ThreadSlot` makes that operation
 * safe at the API level: the first caller to `close` takes the handle, and all
 * later callers observe that the thread has already been closed.
 */
pub struct ThreadSlot<T = ()>(AtomicCell<Option<JoinHandle<T>>>);
impl<T> ThreadSlot<T> {
  pub const fn new(handle: JoinHandle<T>) -> Self {
    Self(AtomicCell::new(Some(handle)))
  }

  pub fn close(&self) -> Option<JoinHandle<T>> {
    self.0.take()
  }
}
