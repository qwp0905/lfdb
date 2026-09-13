use std::{cell::Cell, hint::spin_loop};

const MAX_SPIN: u8 = 5;
pub struct SpinBackoff(Cell<u8>);
impl SpinBackoff {
  pub const fn new() -> Self {
    Self(Cell::new(0))
  }
  pub const fn is_completed(&self) -> bool {
    self.0.get() >= MAX_SPIN
  }
  pub fn spin(&self) {
    let current = self.0.get();
    self.0.set((current + 1).min(MAX_SPIN));
    for _ in 0..1 << current {
      spin_loop();
    }
  }
  pub fn reset(&self) {
    self.0.set(0);
  }
}
