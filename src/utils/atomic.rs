use std::ops::Deref;

use arc_swap::{ArcSwapAny, Guard, RefCnt};

use crate::utils::SBox;

/**
 * Atomically swappable `SBox` slot.
 *
 * `load` briefly takes a shared pin, clones the current `SBox`, and releases
 * the pin. `swap` takes the exclusive pin and replaces the stored `SBox`.
 * After `load` returns, the caller owns an independent strong reference and no
 * longer depends on the slot.
 */
pub struct AtomicSBox<T>(ArcSwapAny<SBox<T>>);
impl<T> AtomicSBox<T> {
  pub fn new(value: T) -> Self {
    Self(ArcSwapAny::new(SBox::new(value)))
  }

  pub fn load(&self) -> AtomicRef<T> {
    AtomicRef(self.0.load())
  }

  pub fn store(&self, value: T) {
    let _ = self.0.swap(SBox::new(value));
  }
  pub fn store_and_load(&self, value: T) -> SBox<T> {
    let value = SBox::new(value);
    let _ = self.0.swap(value.clone());
    value
  }
}

pub struct AtomicRef<T>(Guard<SBox<T>>);
impl<T> Deref for AtomicRef<T> {
  type Target = T;
  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

unsafe impl<T: Send + Sync> Send for AtomicSBox<T> {}
unsafe impl<T: Send + Sync> Sync for AtomicSBox<T> {}

unsafe impl<T> RefCnt for SBox<T> {
  type Base = T;
  fn into_ptr(me: Self) -> *mut Self::Base {
    SBox::into_raw(me)
  }
  fn as_ptr(me: &Self) -> *mut Self::Base {
    me.as_ptr()
  }
  unsafe fn from_ptr(ptr: *const Self::Base) -> Self {
    SBox::from_raw(ptr.cast_mut())
  }
}
