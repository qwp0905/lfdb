use std::{
  mem::forget,
  sync::atomic::{AtomicU32, Ordering},
};

use crossbeam::utils::Backoff;

/*
 * The high bit is the exclusive marker; the remaining bits store the shared
 * holder count.
 *
 * 0              => unlocked
 * 1..EXCLUSIVE-1 => shared holder count
 * EXCLUSIVE      => exclusive holder
 */
const EXCLUSIVE: u32 = 1 << (u32::BITS - 1);

/**
 * Tiny non-blocking reader/exclusive gate.
 *
 * `ExclusivePin` is a small RW-lock-like primitive used for short lifetime
 * guards. Shared tokens increment the reader count, while an exclusive token
 * can be acquired only when there are no shared holders. It does not park or
 * block; callers decide whether to retry, skip, or close.
 *
 * The high bit is the exclusive marker and the remaining bits store the shared
 * holder count.
 */
#[derive(Debug)]
pub struct ExclusivePin(AtomicU32);
impl ExclusivePin {
  #[inline]
  pub const fn new() -> Self {
    Self(AtomicU32::new(0))
  }

  /*
   * `try_shared` fails only when an exclusive holder is observed. If the pin is
   * shareable but the counter CAS races with another shared acquire, it retries
   * briefly until the shared count is incremented.
   */
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

  /*
   * Exclusive acquisition has only one valid transition: 0 -> EXCLUSIVE.
   * Unlike shared acquisition, there is no range of count values to join. If that
   * single transition fails, the caller must decide whether to retry or give up.
   */
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
  /**
   * Try to turn this shared token into the exclusive token.
   *
   * Upgrade succeeds only when this token is the only shared holder. On failure,
   * the original shared token is returned so the caller does not accidentally
   * release its pin. This is useful when a current shared holder should get the
   * first chance to perform the exclusive transition.
   */
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
/*
 * Tokens use lock-style acquire/release ordering.
 *
 * Successful acquisition uses `Acquire`; dropping or downgrading a token uses
 * `Release`. This gives data protected by the pin the usual synchronization
 * boundary expected from a small reader/exclusive gate.
 */
impl<'a> Drop for SharedToken<'a> {
  #[inline]
  fn drop(&mut self) {
    self.0.fetch_sub(1, Ordering::Release);
  }
}

pub struct ExclusiveToken<'a>(&'a AtomicU32);
impl<'a> ExclusiveToken<'a> {
  /**
   * Convert the exclusive token back into one shared token.
   *
   * This is the RW-lock-style downgrade operation: after finishing the exclusive
   * mutation, the caller may continue reading under shared protection without
   * fully releasing the pin.
   */
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
