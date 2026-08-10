use std::{cell::UnsafeCell, thread::yield_now};

const MAX_YIELD: u8 = 10;
pub struct Backoff(UnsafeCell<u8>);
impl Backoff {
  pub const fn new() -> Self {
    Self(UnsafeCell::new(0))
  }
  pub fn snooze(&self) {
    let ptr = self.0.get();
    unsafe { ptr.write((ptr.read() + 1).min(MAX_YIELD)) };
    yield_now();
  }
  pub const fn is_complete(&self) -> bool {
    (unsafe { *self.0.get() }) >= MAX_YIELD
  }
  pub const fn reset(&self) {
    unsafe { self.0.get().write(0) };
  }
}
