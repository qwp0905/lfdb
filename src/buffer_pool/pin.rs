use std::sync::atomic::{AtomicU32, Ordering};

use crossbeam::utils::Backoff;

/**
 * The MSB (bit 31) is the eviction flag; the remaining 31 bits are the
 * pin count. Both are packed into a single atomic so that eviction and
 * pinning can be toggled with a single CAS — analogous to an RwLock
 * where eviction is the write lock and each pin is a read lock.
 */
const EVICTION_BIT: u32 = 1 << (u32::BITS - 1);

pub struct EvictionPin(AtomicU32);
impl EvictionPin {
  #[inline]
  pub fn new() -> Self {
    Self(AtomicU32::new(0))
  }
  #[inline]
  pub fn evicting() -> Self {
    Self(AtomicU32::new(EVICTION_BIT))
  }
  #[inline]
  pub fn init(&self) {
    self.0.store(EVICTION_BIT, Ordering::Release);
  }
  #[inline]
  pub fn try_evict(&self) -> bool {
    self
      .0
      .compare_exchange(0, EVICTION_BIT, Ordering::Acquire, Ordering::Relaxed)
      .is_ok()
  }
  #[inline]
  pub fn try_evict_owned(&self) -> bool {
    self
      .0
      .compare_exchange(1, EVICTION_BIT, Ordering::Acquire, Ordering::Relaxed)
      .is_ok()
  }
  #[inline]
  pub fn try_pin(&self) -> bool {
    let backoff = Backoff::new();
    loop {
      let current = self.0.load(Ordering::Acquire);
      if current & EVICTION_BIT != 0 {
        return false;
      }

      if self
        .0
        .compare_exchange(current, current + 1, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
      {
        return true;
      }
      backoff.spin();
    }
  }

  /**
   * Release eviction state.
   */
  #[inline]
  pub fn release(&self) {
    self.0.store(0, Ordering::Release);
  }
  /**
   * Release eviction state and owned pin to use frame.
   */
  #[inline]
  pub fn owned(&self) {
    self.0.store(1, Ordering::Release);
  }
  /**
   * Releases the eviction lock and sets the pin count to the given value.
   * Safe to use a plain store here because the eviction flag guarantees
   * exclusive access — no other thread can pin or evict while it is set.
   */
  // #[inline]
  // pub fn store(&self, pin: u32) {
  //   self.0.store(pin, Ordering::Release);
  // }
  #[inline]
  pub fn unpin(&self) {
    self.0.fetch_sub(1, Ordering::Release);
  }
}
