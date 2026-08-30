use std::{
  cell::UnsafeCell,
  mem::{forget, MaybeUninit},
  ops::Deref,
  ptr::NonNull,
  sync::atomic::{fence, AtomicBool, Ordering},
  thread::{current, park, Thread},
};

use crossbeam::atomic::AtomicCell;

use crate::utils::Backoff;

#[repr(C)]
struct PairInner<T: ?Sized> {
  dropped: AtomicBool,
  value: T,
}
impl<T> PairInner<T> {
  const fn new(value: T) -> Self {
    Self {
      dropped: AtomicBool::new(false),
      value,
    }
  }
}

pub struct Pair<T: ?Sized>(NonNull<PairInner<T>>);
impl<T> Pair<T> {
  pub fn new(value: T) -> (Self, Self) {
    let inner = PairInner::new(value);
    let ptr = NonNull::from_mut(Box::leak(Box::new(inner)));
    (Self(ptr), Self(ptr))
  }

  pub fn into_raw(this: Self) -> *mut T {
    let ptr = unsafe { &raw mut (*this.0.as_ptr()).value };
    forget(this);
    ptr
  }

  pub unsafe fn from_raw(ptr: *mut T) -> Self {
    let offset = std::mem::offset_of!(PairInner<T>, value);
    let ptr = (ptr as *mut u8).sub(offset) as *mut PairInner<T>;
    Self(NonNull::new_unchecked(ptr))
  }
}
impl<T: ?Sized> Drop for Pair<T> {
  fn drop(&mut self) {
    if !unsafe { self.0.as_ref() }
      .dropped
      .fetch_or(true, Ordering::Release)
    {
      return;
    }
    fence(Ordering::Acquire);
    let _ = unsafe { Box::from_raw(self.0.as_ptr()) };
  }
}
impl<T: ?Sized> Deref for Pair<T> {
  type Target = T;

  fn deref(&self) -> &Self::Target {
    unsafe { &self.0.as_ref().value }
  }
}
unsafe impl<T: Send + Sync + ?Sized> Send for Pair<T> {}
unsafe impl<T: Send + Sync + ?Sized> Sync for Pair<T> {}

pub enum TryWaitError<T> {
  Disconnected,
  Empty(T),
}
#[derive(Debug)]
pub struct WaitDisconnectedError;

/**
 * Creates a single-use channel pair (Oneshot, OneshotFulfill).
 * State transitions: Waiting → Fulfilled → Disconnected.
 * The receiver parks until the sender fulfills the value or disconnects.
 */
pub fn oneshot<T>() -> (Oneshot<T>, OneshotFulfill<T>) {
  let inner = Pair::new(OneshotBehavior::new());
  (Oneshot(inner.0), OneshotFulfill(inner.1))
}

/**
 * State of a single-use completion slot.
 *
 * `Waiting` is the initial state after creating a oneshot. `Fulfilled` means
 * the fulfiller has written the value and completed its side. `Disconnected`
 * means completion is no longer possible or no longer needed: the fulfiller was
 * dropped without writing a value, the waiter was dropped, or the value has
 * already been consumed/cleaned up.
 */
#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
  Waiting,
  Fulfilled,
  Disconnected,
}

/**
 * Two-owner shared allocation used by the oneshot pair.
 *
 * A oneshot always has exactly two handles: the waiter and the fulfiller. The
 * first dropped handle only marks the allocation as disconnected; the second
 * dropped handle reclaims the heap allocation.
 */
pub struct OneshotBehavior<T> {
  state: AtomicCell<State>,
  value: UnsafeCell<MaybeUninit<T>>,
  caller: AtomicCell<Option<Thread>>,
}
impl<T> OneshotBehavior<T> {
  pub const fn new() -> Self {
    Self {
      state: AtomicCell::new(State::Waiting),
      value: UnsafeCell::new(MaybeUninit::uninit()),
      caller: AtomicCell::new(None),
    }
  }

  pub fn fulfill(&self, result: T) {
    let value = unsafe { &mut *self.value.get() };
    value.write(result);
    match self
      .state
      .compare_exchange(State::Waiting, State::Fulfilled)
      .unwrap_or_else(|s| s)
    {
      State::Waiting => {
        let Some(thread) = self.caller.take() else {
          return;
        };
        thread.unpark();
      }
      State::Disconnected => unsafe { value.assume_init_drop() },
      State::Fulfilled => unreachable!(),
    }
  }

  pub fn try_wait(&self) -> std::result::Result<T, TryWaitError<()>> {
    match self
      .state
      .compare_exchange(State::Fulfilled, State::Disconnected)
      .unwrap_or_else(|s| s)
    {
      State::Waiting => Err(TryWaitError::Empty(())),
      State::Fulfilled => Ok(unsafe { (*self.value.get()).assume_init_read() }),
      State::Disconnected => Err(TryWaitError::Disconnected),
    }
  }

  pub fn wait(&self) -> Result<T, WaitDisconnectedError> {
    let backoff = Backoff::new();
    self.caller.store(Some(current()));
    loop {
      match self.try_wait() {
        Ok(v) => return Ok(v),
        Err(TryWaitError::Disconnected) => return Err(WaitDisconnectedError),
        Err(TryWaitError::Empty(_)) => {}
      };
      if !backoff.is_complete() {
        backoff.snooze();
        continue;
      }

      park();
      backoff.reset();
    }
  }

  pub fn drop_receiver(&self) {
    if let State::Fulfilled = self.state.swap(State::Disconnected) {
      unsafe { (*self.value.get()).assume_init_drop() };
    }
  }

  pub fn drop_sender(&self) {
    if self
      .state
      .compare_exchange(State::Waiting, State::Disconnected)
      .is_err()
    {
      return;
    }
    let Some(thread) = self.caller.take() else {
      return;
    };
    thread.unpark();
  }
}

/**
 * Minimal single-use completion primitive for background work.
 *
 * This is not intended to be a general-purpose channel. It exists so
 * `BackgroundThread::execute` can return a cheap handle for receiving exactly
 * one result from a worker. The implementation uses a dedicated heap-allocated
 * pair shared by the waiter and fulfiller to keep the synchronization surface
 * small and predictable.
 */
pub struct Oneshot<T>(Pair<OneshotBehavior<T>>);
impl<T> Oneshot<T> {
  /**
   * Try to consume the result without blocking.
   *
   * `try_wait` takes ownership of the receiver because the oneshot has no valid
   * use after a successful wait or a disconnect. If the value is not ready yet,
   * the receiver is returned in `TryWaitError::Empty` so the caller can try again
   * or fall back to blocking `wait`.
   */
  pub fn try_wait(self) -> std::result::Result<T, TryWaitError<Self>> {
    match self.0.try_wait() {
      Ok(v) => Ok(v),
      Err(TryWaitError::Empty(_)) => Err(TryWaitError::Empty(self)),
      Err(TryWaitError::Disconnected) => Err(TryWaitError::Disconnected),
    }
  }
  pub fn wait(self) -> Result<T, WaitDisconnectedError> {
    self.0.wait()
  }
}
impl<T> Drop for Oneshot<T> {
  fn drop(&mut self) {
    self.0.drop_receiver();
  }
}

pub struct OneshotFulfill<T>(Pair<OneshotBehavior<T>>);
impl<T> OneshotFulfill<T> {
  pub fn fulfill(self, result: T) {
    self.0.fulfill(result);
  }
}
impl<T> Drop for OneshotFulfill<T> {
  fn drop(&mut self) {
    self.0.drop_sender();
  }
}

unsafe impl<T: Send> Sync for OneshotBehavior<T> {}
unsafe impl<T: Send> Send for OneshotBehavior<T> {}

#[cfg(test)]
#[path = "tests/oneshot.rs"]
mod tests;
