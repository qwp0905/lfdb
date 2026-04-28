use std::sync::atomic::{AtomicU64, Ordering};

pub struct Counter(AtomicU64);
impl Counter {
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
}
