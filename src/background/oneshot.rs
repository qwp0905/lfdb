use std::{
  cell::UnsafeCell,
  mem::MaybeUninit,
  ops::Deref,
  sync::atomic::{fence, AtomicBool, Ordering},
  thread::{current, park, yield_now, Thread},
};

use crossbeam::atomic::AtomicCell;

struct Pair<T: ?Sized>(*mut (AtomicBool, T));
impl<T> Pair<T> {
  pub fn new(value: T) -> (Self, Self) {
    let ptr = Box::into_raw(Box::new((AtomicBool::new(false), value)));
    (Self(ptr), Self(ptr))
  }
}
impl<T: ?Sized> Drop for Pair<T> {
  fn drop(&mut self) {
    if !unsafe { &*self.0 }.0.fetch_or(true, Ordering::Release) {
      return;
    }
    fence(Ordering::Acquire);
    let _ = unsafe { Box::from_raw(self.0) };
  }
}
impl<T: ?Sized> Deref for Pair<T> {
  type Target = T;

  fn deref(&self) -> &Self::Target {
    unsafe { &(*self.0).1 }
  }
}
unsafe impl<T: Send + Sync + ?Sized> Send for Pair<T> {}
unsafe impl<T: Send + Sync + ?Sized> Sync for Pair<T> {}

pub enum TryWaitError<T> {
  Disconnected,
  Empty(Oneshot<T>),
}
#[derive(Debug)]
pub struct WaitDisconnectedError;

/**
 * Creates a single-use channel pair (Oneshot, OneshotFulfill).
 * State transitions: Waiting → Fulfilled → Disconnected.
 * The receiver parks until the sender fulfills the value or disconnects.
 */
pub fn oneshot<T>() -> (Oneshot<T>, OneshotFulfill<T>) {
  let inner = Pair::new(OneshotInner::new());
  (Oneshot(inner.0), OneshotFulfill(inner.1))
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
  const fn fulfilled(value: T) -> Self {
    Self {
      state: AtomicCell::new(State::Fulfilled),
      value: UnsafeCell::new(MaybeUninit::new(value)),
      caller: AtomicCell::new(None),
    }
  }
  #[inline]
  const fn get_value(&self) -> &MaybeUninit<T> {
    unsafe { &*self.value.get() }
  }
}

const MAX_YIELD: usize = 10;

pub struct Oneshot<T>(Pair<OneshotInner<T>>);
impl<T> Oneshot<T> {
  pub fn fulfilled(value: T) -> Self {
    let inner = OneshotInner::fulfilled(value);
    let (inner, _) = Pair::new(inner);
    Oneshot(inner)
  }
  pub fn try_wait(self) -> std::result::Result<T, TryWaitError<T>> {
    match self
      .0
      .state
      .compare_exchange(State::Fulfilled, State::Disconnected)
      .unwrap_or_else(|s| s)
    {
      State::Waiting => Err(TryWaitError::Empty(self)),
      State::Fulfilled => Ok(unsafe { self.0.get_value().assume_init_read() }),
      State::Disconnected => Err(TryWaitError::Disconnected),
    }
  }
  pub fn wait(mut self) -> Result<T, WaitDisconnectedError> {
    let mut backoff = 0;
    // Register the caller thread before checking state. If fulfill() runs
    // first and finds caller as None, it won't call unpark() — causing park()
    // to block forever.
    self.0.caller.store(Some(current()));
    loop {
      match self.try_wait() {
        Ok(v) => return Ok(v),
        Err(TryWaitError::Disconnected) => return Err(WaitDisconnectedError),
        Err(TryWaitError::Empty(this)) => self = this,
      };
      if backoff < MAX_YIELD {
        backoff += 1;
        yield_now();
        continue;
      }

      park();
      backoff = 0;
    }
  }
}
impl<T> Drop for Oneshot<T> {
  fn drop(&mut self) {
    if let State::Fulfilled = self.0.state.swap(State::Disconnected) {
      unsafe { (*self.0.value.get()).assume_init_drop() };
    }
  }
}

pub struct OneshotFulfill<T>(Pair<OneshotInner<T>>);
impl<T> OneshotFulfill<T> {
  pub fn fulfill(self, result: T) {
    let value = unsafe { &mut *self.0.value.get() };
    value.write(result);
    match self
      .0
      .state
      .compare_exchange(State::Waiting, State::Fulfilled)
      .unwrap_or_else(|s| s)
    {
      State::Waiting => {
        if let Some(th) = self.0.caller.take() {
          th.unpark()
        }
      }
      State::Disconnected => unsafe { value.assume_init_drop() },
      State::Fulfilled => unreachable!(),
    }
  }
}
impl<T> Drop for OneshotFulfill<T> {
  fn drop(&mut self) {
    let Ok(_) = self
      .0
      .state
      .compare_exchange(State::Waiting, State::Disconnected)
    else {
      return;
    };
    let Some(th) = self.0.caller.take() else {
      return;
    };
    th.unpark();
  }
}

unsafe impl<T: Send> Sync for OneshotInner<T> {}
unsafe impl<T: Send> Send for OneshotInner<T> {}

#[cfg(test)]
#[path = "tests/oneshot.rs"]
mod tests;
