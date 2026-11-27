use std::{
  ops::Deref,
  sync::{atomic::Ordering, Once},
};

use crate::utils::{
  loom::{
    fence,
    thread::{current, park, Thread},
    AtomicU8,
  },
  MsQueue, PopGuard, PushGuard,
};

/**
 * A tiny one-shot wake-all primitive.
 *
 * `park` waits until the parker is opened, and `wake_all` opens it
 * permanently. Once opened, all current waiters are released and future calls
 * to `park` return immediately.
 *
 * This is a small utility around `Once` used as a wake-all latch. It is not
 * tied to a specific background runtime and could live in the general utility
 * module. Poisoning is intentionally ignored: a panic while opening the latch is
 * treated as a severe programming error, not as a recoverable state.
 */
pub struct OnceParker(Once);

impl OnceParker {
  pub const fn new() -> Self {
    Self(Once::new())
  }

  pub fn park(&self) {
    self.0.wait_force();
  }

  pub fn wake_all(&self) {
    self.0.call_once_force(|_| ());
  }
}
impl Default for OnceParker {
  fn default() -> Self {
    Self::new()
  }
}

const STATE_QUEUED: u8 = 0;
const STATE_PARKED: u8 = 1;
const STATE_CANCELED: u8 = 2;

pub(crate) struct Waker {
  state: AtomicU8,
  thread: Thread,
}
impl Waker {
  fn new() -> Self {
    Self {
      state: AtomicU8::new(STATE_QUEUED),
      thread: current(),
    }
  }

  fn try_park(&self) -> bool {
    if self
      .state
      .compare_exchange(
        STATE_QUEUED,
        STATE_PARKED,
        Ordering::Relaxed,
        Ordering::Relaxed,
      )
      .is_err()
    {
      return false;
    }
    park();

    let _ = self.state.compare_exchange(
      STATE_PARKED,
      STATE_CANCELED,
      Ordering::Relaxed,
      Ordering::Relaxed,
    );
    true
  }
  fn try_wake(&self) -> bool {
    match self.state.swap(STATE_CANCELED, Ordering::Relaxed) {
      STATE_QUEUED => return true,
      STATE_PARKED => {}
      STATE_CANCELED => return false,
      _ => unreachable!(),
    };
    self.thread.unpark();
    true
  }

  fn try_cancel(&self) -> bool {
    self
      .state
      .compare_exchange(
        STATE_QUEUED,
        STATE_CANCELED,
        Ordering::Relaxed,
        Ordering::Relaxed,
      )
      .is_ok()
  }
}

/**
 * The parker relies on the queue only for happens-before from push to the pop
 * that returns the value, so it stays correct regardless of the implementation.
 */
pub(crate) trait WakerQueue<T>: Default {
  type PushRef<'a>: Deref<Target = T>
  where
    Self: 'a;
  type PopRef<'a>: Deref<Target = T>
  where
    Self: 'a;
  fn push(&self, value: T) -> Self::PushRef<'_>;
  fn pop(&self) -> Option<Self::PopRef<'_>>;
}
impl<T> WakerQueue<T> for MsQueue<T> {
  type PushRef<'a>
    = PushGuard<T>
  where
    Self: 'a;
  type PopRef<'a>
    = PopGuard<T>
  where
    Self: 'a;

  fn push(&self, value: T) -> Self::PushRef<'_> {
    unsafe { self.push_then(value) }
  }

  fn pop(&self) -> Option<Self::PopRef<'_>> {
    self.pop_then()
  }
}

pub struct ThreadParker<Q: WakerQueue<Waker> = MsQueue<Waker>> {
  queue: Q,
}
impl<Q: WakerQueue<Waker>> ThreadParker<Q> {
  pub fn new() -> Self {
    Self {
      queue: Q::default(),
    }
  }

  pub fn wake_all(&self) {
    fence(Ordering::SeqCst);
    while let Some(waker) = self.queue.pop() {
      waker.try_wake();
    }
  }

  pub fn wake_once(&self) {
    fence(Ordering::SeqCst);
    while let Some(waker) = self.queue.pop() {
      if waker.try_wake() {
        return;
      }
    }
  }
  pub fn park_or_cancel<T>(&self, has_next: impl FnOnce() -> Option<T>) -> Option<T> {
    let waker = Waker::new();
    let guard = self.queue.push(waker);
    // Pairs with the fence in wake, so either the waker finds this entry or
    // has_next sees the work published before the wake.
    fence(Ordering::SeqCst);
    let Some(next) = has_next() else {
      guard.try_park();
      return None;
    };
    if !guard.try_cancel() {
      self.wake_once();
    }
    Some(next)
  }
}
impl<Q: WakerQueue<Waker>> Drop for ThreadParker<Q> {
  fn drop(&mut self) {
    self.wake_all();
  }
}

#[cfg(all(test, loom))]
#[path = "tests/parker_loom.rs"]
mod tests;
