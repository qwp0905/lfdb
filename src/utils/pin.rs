use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam::utils::Backoff;

const EXCLUSIVE: usize = 1 << (usize::BITS - 1);
pub struct ExclusivePin(AtomicUsize);
impl ExclusivePin {
  #[inline]
  pub fn new() -> Self {
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
        return Some(SharedToken::new(&self.0));
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
      .map(|_| ExclusiveToken::new(&self.0))
  }

  #[inline]
  pub fn is_exclusive(&self) -> bool {
    self.0.load(Ordering::Acquire) == EXCLUSIVE
  }
}

pub struct SharedToken<'a> {
  pin: &'a AtomicUsize,
  upgrade: bool,
}
impl<'a> SharedToken<'a> {
  #[inline]
  fn new(pin: &'a AtomicUsize) -> Self {
    Self {
      pin,
      upgrade: false,
    }
  }
  pub fn upgrade(mut self) -> ExclusiveToken<'a> {
    let backoff = Backoff::new();

    while let Err(_) =
      self
        .pin
        .compare_exchange(1, EXCLUSIVE, Ordering::Acquire, Ordering::Relaxed)
    {
      backoff.snooze();
    }
    self.upgrade = true;
    ExclusiveToken::new(self.pin)
  }
}
impl<'a> Drop for SharedToken<'a> {
  #[inline]
  fn drop(&mut self) {
    if self.upgrade {
      return;
    }
    self.pin.fetch_sub(1, Ordering::Release);
  }
}

pub struct ExclusiveToken<'a> {
  pin: &'a AtomicUsize,
  downgrade: bool,
}
impl<'a> ExclusiveToken<'a> {
  #[inline]
  fn new(pin: &'a AtomicUsize) -> Self {
    Self {
      pin,
      downgrade: false,
    }
  }

  #[inline]
  pub fn downgrade(mut self) -> SharedToken<'a> {
    self.downgrade = true;
    self.pin.store(1, Ordering::Release);
    SharedToken::new(self.pin)
  }
}
impl<'a> Drop for ExclusiveToken<'a> {
  #[inline]
  fn drop(&mut self) {
    if self.downgrade {
      return;
    }
    self.pin.store(0, Ordering::Release);
  }
}

#[cfg(test)]
#[path = "tests/pin.rs"]
mod tests;
