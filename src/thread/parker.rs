use std::sync::Once;

pub struct OnceParker {
  once: Once,
}

impl OnceParker {
  pub const fn new() -> Self {
    Self { once: Once::new() }
  }

  pub fn park(&self) {
    self.once.wait();
  }

  pub fn wake_all(&self) {
    self.once.call_once(|| {});
  }
}
