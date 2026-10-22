#[cfg(not(loom))]
pub use crossbeam::utils::Backoff;
#[cfg(not(loom))]
pub use std::sync::atomic::AtomicU32;

#[cfg(loom)]
pub use ::loom::sync::atomic::AtomicU32;

/*
 * Spinning and yielding differ only in cost, so every backoff step is modeled
 * as a wait; `loom::hint::spin_loop` yields to the model scheduler.
 */
#[cfg(loom)]
pub struct Backoff;
#[cfg(loom)]
impl Backoff {
  pub const fn new() -> Self {
    Self
  }

  pub fn spin(&self) {
    ::loom::hint::spin_loop();
  }
}
