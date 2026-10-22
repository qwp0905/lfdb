#[cfg(not(loom))]
pub use crossbeam::utils::Backoff;
#[cfg(not(loom))]
pub use std::sync::atomic::{fence, AtomicU32, AtomicUsize};

#[cfg(loom)]
pub use ::loom::sync::atomic::{fence, AtomicU32, AtomicUsize};

/*
 * Spinning and yielding differ only in cost, not in correctness, so the model
 * can treat every backoff step as a wait for other threads.
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
