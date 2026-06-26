use std::cell::UnsafeCell;

use crossbeam::utils::Backoff;

use crate::utils::SBox;

use super::ExclusivePin;

/**
 * Atomically swappable `SBox` slot.
 *
 * `load` briefly takes a shared pin, clones the current `SBox`, and releases
 * the pin. `swap` takes the exclusive pin and replaces the stored `SBox`.
 * After `load` returns, the caller owns an independent strong reference and no
 * longer depends on the slot.
 */
pub struct AtomicSBox<T> {
  /*
   * The `UnsafeCell` is protected by `ExclusivePin`.
   *
   * Readers hold a shared token only long enough to clone the current `SBox`.
   * Writers hold the exclusive token only long enough to replace it. Since these
   * critical sections are intentionally tiny, contention is handled with a short
   * backoff loop instead of an OS lock.
   */
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

unsafe impl<T: Send + Sync> Send for AtomicSBox<T> {}
unsafe impl<T: Send + Sync> Sync for AtomicSBox<T> {}
