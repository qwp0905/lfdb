use std::sync::atomic::{AtomicU64, Ordering};

pub struct Gauge(AtomicU64);
impl Gauge {
  pub const fn new() -> Self {
    Self(AtomicU64::new(0))
  }

  #[inline]
  pub fn load(&self) -> u64 {
    self.0.load(Ordering::Relaxed)
  }

  #[inline]
  pub fn inc(&self) {
    self.0.fetch_add(1, Ordering::Relaxed);
  }

  #[inline]
  pub fn dec(&self) {
    self.0.fetch_sub(1, Ordering::Relaxed);
  }
}
