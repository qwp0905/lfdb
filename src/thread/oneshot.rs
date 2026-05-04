use std::{
  cell::UnsafeCell,
  mem::MaybeUninit,
  panic::{RefUnwindSafe, UnwindSafe},
  sync::Arc,
  thread::{current, park, Thread},
};

use crossbeam::atomic::AtomicCell;

use crate::{utils::ToArc, Error, Result};

/**
 * Creates a single-use channel pair (Oneshot, OneshotFulfill).
 * State transitions: Waiting → Fulfilled → Disconnected.
 * The receiver parks until the sender fulfills the value or disconnects.
 */
pub fn oneshot<T>() -> (Oneshot<T>, OneshotFulfill<T>) {
  let inner = OneshotInner::new().to_arc();
  (Oneshot(inner.clone()), OneshotFulfill(inner))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
  Waiting,
  Fulfilled,
  Disconnected,
}

/**
 * value is uninitialized memory — safe to access only when state is Fulfilled,
 * which is enforced by the state machine.
 */
struct OneshotInner<T> {
  state: AtomicCell<State>,
  value: UnsafeCell<MaybeUninit<T>>,
  caller: AtomicCell<Option<Thread>>,
}
impl<T> OneshotInner<T> {
  const fn new() -> Self {
    Self {
      state: AtomicCell::new(State::Waiting),
      value: UnsafeCell::new(MaybeUninit::uninit()),
      caller: AtomicCell::new(None),
    }
  }
  #[inline]
  const fn get_value(&self) -> &MaybeUninit<T> {
    unsafe { &*self.value.get() }
  }
  #[inline]
  const fn get_value_mut(&self) -> &mut MaybeUninit<T> {
    unsafe { &mut *self.value.get() }
  }
}

pub struct Oneshot<T>(Arc<OneshotInner<T>>);
impl<T> Oneshot<T> {
  pub fn wait(self) -> Result<T> {
    // Register the caller thread before checking state. If fulfill() runs
    // first and finds caller as None, it won't call unpark() — causing park()
    // to block forever.
    self.0.caller.store(Some(current()));
    loop {
      match self
        .0
        .state
        .compare_exchange(State::Fulfilled, State::Disconnected)
        .unwrap_or_else(|s| s)
      {
        State::Fulfilled => return Ok(unsafe { self.0.get_value().assume_init_read() }),
        State::Waiting => park(),
        State::Disconnected => return Err(Error::ChannelDisconnected),
      }
    }
  }
}
impl<T> Drop for Oneshot<T> {
  fn drop(&mut self) {
    if let State::Fulfilled = self.0.state.swap(State::Disconnected) {
      unsafe { self.0.get_value_mut().assume_init_drop() };
    }
  }
}

pub struct OneshotFulfill<T>(Arc<OneshotInner<T>>);
impl<T> OneshotFulfill<T> {
  pub fn fulfill(self, result: T) {
    let value = self.0.get_value_mut();
    value.write(result);
    match self
      .0
      .state
      .compare_exchange(State::Waiting, State::Fulfilled)
      .unwrap_or_else(|s| s)
    {
      State::Waiting => match self.0.caller.take() {
        Some(th) => th.unpark(),
        None => return,
      },
      State::Disconnected => unsafe { value.assume_init_drop() },
      State::Fulfilled => unreachable!(),
    }
  }
}
impl<T> Drop for OneshotFulfill<T> {
  fn drop(&mut self) {
    self
      .0
      .state
      .compare_exchange(State::Waiting, State::Disconnected)
      .ok()
      .and_then(|_| self.0.caller.take())
      .map(|th| th.unpark());
  }
}

// OneshotFulfill is sent to worker threads, so Send+Sync are required.
// Oneshot stays on the requesting thread and never crosses thread boundaries.
unsafe impl<T: Send> Sync for OneshotInner<T> {}
unsafe impl<T: Send> Send for OneshotInner<T> {}
impl<T> UnwindSafe for OneshotInner<T> {}
impl<T> RefUnwindSafe for OneshotInner<T> {}

#[cfg(test)]
#[path = "tests/oneshot.rs"]
mod tests;
