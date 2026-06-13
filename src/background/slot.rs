use std::{
  cell::UnsafeCell,
  sync::atomic::{AtomicBool, Ordering},
  thread::JoinHandle,
};

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
    if self.closed.fetch_or(true, Ordering::Release) {
      return None;
    }
    unsafe { (*self.handle.get()).take() }
  }
}
unsafe impl<T: Send> Send for ThreadSlot<T> {}
unsafe impl<T: Send + Sync> Sync for ThreadSlot<T> {}
