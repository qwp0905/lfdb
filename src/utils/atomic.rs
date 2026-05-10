use std::{
  cell::UnsafeCell,
  panic::{RefUnwindSafe, UnwindSafe},
};

use crossbeam::utils::Backoff;

use super::{ExclusivePin, SArc};

pub struct AtomicArc<T> {
  value: UnsafeCell<SArc<T>>,
  lock: ExclusivePin,
}
impl<T> AtomicArc<T> {
  pub fn new(value: T) -> Self {
    Self {
      value: UnsafeCell::new(SArc::new(value)),
      lock: ExclusivePin::new(),
    }
  }

  pub fn load(&self) -> SArc<T> {
    let backoff = Backoff::new();
    loop {
      if let Some(_token) = self.lock.try_shared() {
        return unsafe { &*self.value.get() }.clone();
      }
      backoff.snooze();
    }
  }

  pub fn swap(&self, value: T) -> SArc<T> {
    let value = SArc::new(value);
    let backoff = Backoff::new();
    loop {
      if let Some(_token) = self.lock.try_exclusive() {
        return unsafe { self.value.get().replace(value) };
      }
      backoff.snooze();
    }
  }

  #[inline]
  pub fn store(&self, value: T) {
    let _ = self.swap(value);
  }
}

unsafe impl<T: Send> Send for AtomicArc<T> {}
unsafe impl<T: Send> Sync for AtomicArc<T> {}
impl<T: RefUnwindSafe> RefUnwindSafe for AtomicArc<T> {}
impl<T: UnwindSafe> UnwindSafe for AtomicArc<T> {}
