use std::{
  mem::forget,
  sync::atomic::{AtomicU32, Ordering},
};

use crossbeam::utils::Backoff;

const EXCLUSIVE: u32 = 1 << (u32::BITS - 1);

#[derive(Debug)]
pub struct ExclusivePin(AtomicU32);
impl ExclusivePin {
  #[inline]
  pub const fn new() -> Self {
    Self(AtomicU32::new(0))
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

pub struct SharedToken<'a>(&'a AtomicU32);
impl<'a> SharedToken<'a> {
  pub fn try_upgrade(self) -> std::result::Result<ExclusiveToken<'a>, Self> {
    let pin = self.0;
    if pin
      .compare_exchange(1, EXCLUSIVE, Ordering::Acquire, Ordering::Relaxed)
      .is_err()
    {
      return Err(self);
    }

    forget(self);
    Ok(ExclusiveToken(pin))
  }
}
impl<'a> Drop for SharedToken<'a> {
  #[inline]
  fn drop(&mut self) {
    self.0.fetch_sub(1, Ordering::Release);
  }
}

pub struct ExclusiveToken<'a>(&'a AtomicU32);
impl<'a> ExclusiveToken<'a> {
  #[inline]
  pub fn downgrade(self) -> SharedToken<'a> {
    let pin = self.0;
    debug_assert_eq!(pin.load(Ordering::Acquire), EXCLUSIVE);

    forget(self);
    pin.store(1, Ordering::Release);

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
