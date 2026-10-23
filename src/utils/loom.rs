/**
 * Loom explores only operations performed through its own types, so primitives
 * verified with loom take their synchronization types from here. The std build
 * exposes the same shapes so call sites stay identical in both builds.
 */
#[cfg(not(loom))]
pub use crossbeam::utils::Backoff;
#[cfg(not(loom))]
pub use std::sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicU32, AtomicUsize};

#[cfg(loom)]
pub use ::loom::cell::UnsafeCell;
#[cfg(loom)]
pub use ::loom::sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicU32, AtomicUsize};

#[cfg(not(loom))]
pub struct UnsafeCell<T>(std::cell::UnsafeCell<T>);
#[cfg(not(loom))]
impl<T> UnsafeCell<T> {
  #[inline]
  pub fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
    f(self.0.get())
  }

  #[inline]
  pub fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
    f(self.0.get())
  }
}

/*
 * Spinning and yielding differ only in cost, not in correctness, so every step
 * is treated as a wait for other threads.
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

  pub fn snooze(&self) {
    ::loom::thread::yield_now();
  }
}
