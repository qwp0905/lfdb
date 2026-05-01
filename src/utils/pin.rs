use std::{
  mem::forget,
  sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam::utils::Backoff;

const EXCLUSIVE: usize = 1 << (usize::BITS - 1);

#[derive(Debug)]
pub struct ExclusivePin(AtomicUsize);
impl ExclusivePin {
  #[inline]
  pub const fn new() -> Self {
    Self(AtomicUsize::new(0))
  }

  pub fn try_shared(&self) -> Option<SharedToken<'_>> {
    let backoff = Backoff::new();
    loop {
      let current = self.0.load(Ordering::Acquire);
      if current & EXCLUSIVE != 0 {
        return None;
      }

      if self
        .0
        .compare_exchange(current, current + 1, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
      {
        return Some(SharedToken(&self.0));
      }
      backoff.spin();
    }
  }

  #[inline]
  pub fn try_exclusive(&self) -> Option<ExclusiveToken<'_>> {
    self
      .0
      .compare_exchange(0, EXCLUSIVE, Ordering::Acquire, Ordering::Relaxed)
      .ok()
      .map(|_| ExclusiveToken(&self.0))
  }

  #[inline]
  #[allow(dead_code)]
  pub fn is_exclusive(&self) -> bool {
    self.0.load(Ordering::Acquire) == EXCLUSIVE
  }
}

pub struct SharedToken<'a>(&'a AtomicUsize);
impl<'a> SharedToken<'a> {
  pub fn upgrade(self) -> ExclusiveToken<'a> {
    let backoff = Backoff::new();
    let pin = self.0;

    while let Err(_) =
      pin.compare_exchange(1, EXCLUSIVE, Ordering::Acquire, Ordering::Relaxed)
    {
      backoff.snooze();
    }

    forget(self);
    ExclusiveToken(pin)
  }
}
impl<'a> Drop for SharedToken<'a> {
  #[inline]
  fn drop(&mut self) {
    self.0.fetch_sub(1, Ordering::Release);
  }
}

pub struct ExclusiveToken<'a>(&'a AtomicUsize);
impl<'a> ExclusiveToken<'a> {
  #[inline]
  pub fn downgrade(self) -> SharedToken<'a> {
    let pin = self.0;
    debug_assert_eq!(pin.load(Ordering::Acquire), EXCLUSIVE);

    pin.store(1, Ordering::Release);
    forget(self);

    SharedToken(pin)
  }
}
impl<'a> Drop for ExclusiveToken<'a> {
  #[inline]
  fn drop(&mut self) {
    debug_assert_eq!(self.0.load(Ordering::Acquire), EXCLUSIVE);
    self.0.store(0, Ordering::Release);
  }
}

#[cfg(test)]
#[path = "tests/pin.rs"]
mod tests;
