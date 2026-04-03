use std::{
  cell::UnsafeCell,
  ops::{Deref, DerefMut},
  panic::{RefUnwindSafe, UnwindSafe},
  sync::atomic::{AtomicU32, Ordering},
};

use crossbeam::utils::Backoff;

use crate::utils::{UnsafeBorrow, UnsafeBorrowMut};

const WRITE_BIT: u32 = 1 << 31;

pub struct SpinRwLock<T> {
  value: UnsafeCell<T>,
  pin: AtomicU32,
}
impl<T> SpinRwLock<T> {
  pub fn new(value: T) -> Self {
    Self {
      value: UnsafeCell::new(value),
      pin: AtomicU32::new(0),
    }
  }

  pub fn read<'a, 'b: 'a>(&'b self) -> SpinRwLockReadGuard<'a, T> {
    let backoff = Backoff::new();
    loop {
      let cur = self.pin.load(Ordering::Acquire);
      if cur & WRITE_BIT != 0 {
        backoff.spin();
        continue;
      }

      if self
        .pin
        .compare_exchange(cur, cur + 1, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
      {
        return SpinRwLockReadGuard {
          value: self.value.get().borrow_unsafe(),
          pin: &self.pin,
        };
      }

      backoff.spin()
    }
  }

  pub fn write<'a, 'b: 'a>(&'b self) -> SpinRwLockWriteGuard<'a, T> {
    let backoff = Backoff::new();
    loop {
      if self
        .pin
        .compare_exchange(0, WRITE_BIT, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
      {
        return SpinRwLockWriteGuard {
          value: self.value.get().borrow_mut_unsafe(),
          pin: &self.pin,
        };
      }

      backoff.spin()
    }
  }
}
impl<T: Default> Default for SpinRwLock<T> {
  fn default() -> Self {
    Self::new(Default::default())
  }
}
unsafe impl<T: Send> Send for SpinRwLock<T> {}
unsafe impl<T: Send + Sync> Sync for SpinRwLock<T> {}
impl<T: UnwindSafe> UnwindSafe for SpinRwLock<T> {}
impl<T: RefUnwindSafe> RefUnwindSafe for SpinRwLock<T> {}

pub struct SpinRwLockReadGuard<'a, T> {
  value: &'a T,
  pin: &'a AtomicU32,
}
pub struct SpinRwLockWriteGuard<'a, T> {
  value: &'a mut T,
  pin: &'a AtomicU32,
}
impl<'a, T> Deref for SpinRwLockReadGuard<'a, T> {
  type Target = T;

  #[inline]
  fn deref(&self) -> &Self::Target {
    self.value
  }
}
impl<'a, T> Drop for SpinRwLockReadGuard<'a, T> {
  #[inline]
  fn drop(&mut self) {
    self.pin.fetch_sub(1, Ordering::Release);
  }
}
impl<'a, T> Deref for SpinRwLockWriteGuard<'a, T> {
  type Target = T;

  #[inline]
  fn deref(&self) -> &Self::Target {
    self.value
  }
}
impl<'a, T> DerefMut for SpinRwLockWriteGuard<'a, T> {
  #[inline]
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.value
  }
}
impl<'a, T> Drop for SpinRwLockWriteGuard<'a, T> {
  #[inline]
  fn drop(&mut self) {
    self.pin.store(0, Ordering::Release);
  }
}

#[cfg(test)]
#[path = "tests/spin.rs"]
mod tests;
