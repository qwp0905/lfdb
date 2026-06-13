use std::cell::UnsafeCell;

use crossbeam::utils::Backoff;

use crate::utils::SBox;

use super::ExclusivePin;

pub struct AtomicSBox<T> {
  value: UnsafeCell<SBox<T>>,
  lock: ExclusivePin,
}
impl<T> AtomicSBox<T> {
  pub fn new(value: T) -> Self {
    Self {
      value: UnsafeCell::new(SBox::new(value)),
      lock: ExclusivePin::new(),
    }
  }

  pub fn load(&self) -> SBox<T> {
    let backoff = Backoff::new();
    loop {
      if let Some(_token) = self.lock.try_shared() {
        return unsafe { &*self.value.get() }.clone();
      }
      backoff.snooze();
    }
  }

  pub fn swap(&self, value: T) -> SBox<T> {
    let value = SBox::new(value);
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

unsafe impl<T: Send> Send for AtomicSBox<T> {}
unsafe impl<T: Send> Sync for AtomicSBox<T> {}
