use std::{
  cell::{OnceCell, UnsafeCell},
  mem::{forget, MaybeUninit},
  ops::Deref,
  ptr::{without_provenance_mut, NonNull},
  sync::{
    atomic::{fence, AtomicBool, AtomicPtr, Ordering},
    Arc,
  },
  thread::{current, park, Thread},
};

use crossbeam::utils::Backoff;

use crate::utils::SBox;

use super::{CallbackSlot, WaitHistory};

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

  pub const fn into_raw(this: Self) -> *mut T {
    let ptr = unsafe { &raw mut (*this.0.as_ptr()).value };
    forget(this);
    ptr
  }

  pub const unsafe fn from_raw(ptr: *mut T) -> Self {
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

#[repr(align(4))]
struct ThreadWaker(Thread);
impl ThreadWaker {
  fn new() -> Self {
    Self(current())
  }

  fn wake(&self) {
    self.0.unpark();
  }
}

thread_local! {
  static CURRENT: OnceCell<SBox<ThreadWaker>> = const { OnceCell::new() };
}

struct WakerRef {
  moved: bool,
  ptr: *mut ThreadWaker,
}
impl WakerRef {
  fn new() -> Self {
    let waker = CURRENT.with(|v| v.get_or_init(|| SBox::new(ThreadWaker::new())).clone());
    Self {
      moved: false,
      ptr: SBox::into_raw(waker),
    }
  }

  const fn set_moved(&mut self) {
    self.moved = true;
  }
  const fn as_ptr(&self) -> *mut ThreadWaker {
    self.ptr
  }
}
impl Drop for WakerRef {
  fn drop(&mut self) {
    if self.moved {
      return;
    }
    let _ = unsafe { SBox::from_raw(self.ptr) };
  }
}

// Tagged pointers
const STATE_WAITING: *mut ThreadWaker = without_provenance_mut(0);
const STATE_FULFILLED: *mut ThreadWaker = without_provenance_mut(1);
const STATE_DISCONNECTED: *mut ThreadWaker = without_provenance_mut(2);

struct Atomic<T>(AtomicPtr<T>);
impl<T> Atomic<T> {
  const fn new(ptr: *mut T) -> Self {
    Self(AtomicPtr::new(ptr))
  }
  fn load(&self) -> *mut T {
    self.0.load(Ordering::Acquire)
  }
  fn cas_weak(
    &self,
    current: *mut T,
    new: *mut T,
  ) -> std::result::Result<*mut T, *mut T> {
    self
      .0
      .compare_exchange_weak(current, new, Ordering::Release, Ordering::Acquire)
  }

  fn swap(&self, new: *mut T) -> *mut T {
    self.0.swap(new, Ordering::AcqRel)
  }
}

/**
 * Two-owner shared allocation used by the oneshot pair.
 *
 * A oneshot always has exactly two handles: the waiter and the fulfiller. The
 * first dropped handle only marks the allocation as disconnected; the second
 * dropped handle reclaims the heap allocation.
 */
pub struct OneshotBehavior<T> {
  state: Atomic<ThreadWaker>,
  value: UnsafeCell<MaybeUninit<T>>,
  callback: CallbackSlot,
  history: Arc<WaitHistory>,
}
impl<T> OneshotBehavior<T> {
  const fn new(history: Arc<WaitHistory>) -> Self {
    Self {
      value: UnsafeCell::new(MaybeUninit::uninit()),
      state: Atomic::new(STATE_WAITING),
      callback: CallbackSlot::new(),
      history,
    }
  }

  pub fn add_callback<F: FnOnce(&T) + Send + 'static>(
    &self,
    f: F,
  ) -> std::result::Result<(), F> {
    self.callback.set(f)
  }

  pub unsafe fn fulfill(&self, result: T) {
    if let Some(callback) = self.callback.take() {
      unsafe { callback.call(&result) };
    }
    unsafe { (*self.value.get()).write(result) };
  }

  pub unsafe fn wake(this: *const Self) {
    let backoff = Backoff::new();
    let mut state = (*this).state.load();
    loop {
      if state == STATE_DISCONNECTED {
        return unsafe { (*this).drop_value() };
      }

      if let Err(err) = (*this).state.cas_weak(state, STATE_FULFILLED) {
        backoff.spin();
        state = err;
        continue;
      }

      return match state {
        STATE_DISCONNECTED | STATE_FULFILLED => unreachable!(),
        STATE_WAITING => {}
        _ => unsafe { SBox::from_raw(state) }.wake(),
      };
    }
  }

  pub fn fulfill_and_wake(this: *const Self, result: T) {
    unsafe {
      (*this).fulfill(result);
      Self::wake(this);
    }
  }

  fn try_wait(&self) -> std::result::Result<T, TryWaitError<()>> {
    let backoff = Backoff::new();
    let mut state = self.state.load();
    loop {
      match state {
        STATE_DISCONNECTED => return Err(TryWaitError::Disconnected),
        STATE_FULFILLED => {}
        _ => return Err(TryWaitError::Empty(())),
      }
      let Err(err) = self.state.cas_weak(state, STATE_DISCONNECTED) else {
        return Ok(unsafe { self.read_value() });
      };
      backoff.spin();
      state = err;
    }
  }

  const unsafe fn read_value(&self) -> T {
    unsafe { (*self.value.get()).assume_init_read() }
  }
  unsafe fn drop_value(&self) {
    unsafe { (*self.value.get()).assume_init_drop() };
  }

  fn try_park_with(&self, waker: &mut WakerRef) -> Result<bool, WaitDisconnectedError> {
    let backoff = Backoff::new();
    let mut state = self.state.load();
    loop {
      match state {
        STATE_FULFILLED => {
          let Err(err) = self.state.cas_weak(state, STATE_DISCONNECTED) else {
            return Ok(false);
          };
          backoff.spin();
          state = err;
        }
        STATE_DISCONNECTED => return Err(WaitDisconnectedError),
        STATE_WAITING => {
          let Err(err) = self.state.cas_weak(state, waker.as_ptr()) else {
            waker.set_moved();
            return Ok(true);
          };
          backoff.spin();
          state = err;
        }
        _ => return Ok(true),
      }
    }
  }

  pub fn wait(&self) -> Result<T, WaitDisconnectedError> {
    let mut waker = WakerRef::new();
    let backoff = self.history.current();
    loop {
      while !backoff.is_completed() {
        match self.try_wait() {
          Ok(v) => return Ok(v),
          Err(TryWaitError::Disconnected) => return Err(WaitDisconnectedError),
          Err(TryWaitError::Empty(_)) => backoff.snooze(),
        };
      }
      if !self.try_park_with(&mut waker)? {
        return Ok(unsafe { self.read_value() });
      };
      park();
    }
  }

  pub fn drop_receiver(&self) {
    match self.state.swap(STATE_DISCONNECTED) {
      STATE_FULFILLED => unsafe { self.drop_value() },
      STATE_WAITING | STATE_DISCONNECTED => {}
      state => {
        let _ = unsafe { SBox::from_raw(state) };
      }
    }
  }

  pub fn drop_sender(&self) {
    let backoff = Backoff::new();
    let mut state = self.state.load();
    loop {
      if matches!(state, STATE_FULFILLED | STATE_DISCONNECTED) {
        return;
      }

      if let Err(err) = self.state.cas_weak(state, STATE_DISCONNECTED) {
        backoff.spin();
        state = err;
        continue;
      }
      if state != STATE_WAITING {
        unsafe { SBox::from_raw(state) }.wake();
      }
      return;
    }
  }
}

unsafe impl<T: Send> Sync for OneshotBehavior<T> {}
unsafe impl<T: Send> Send for OneshotBehavior<T> {}

/**
 * Minimal single-use completion primitive for background work.
 *
 * This is not intended to be a general-purpose channel. It exists so
 * `Execute::execute` can return a cheap handle for receiving exactly
 * one result from a worker. The implementation uses a dedicated heap-allocated
 * pair shared by the waiter and fulfiller to keep the synchronization surface
 * small and predictable.
 */
pub struct Oneshot<T>(Pair<OneshotBehavior<T>>);
impl<T> Oneshot<T> {
  pub fn wait(self) -> Result<T, WaitDisconnectedError> {
    self.0.wait()
  }

  pub fn add_callback<F: FnOnce(&T) + Send + 'static>(
    &self,
    f: F,
  ) -> std::result::Result<(), F> {
    self.0.add_callback(f)
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
    OneshotBehavior::fulfill_and_wake(&*self.0 as _, result);
  }
}
impl<T> Drop for OneshotFulfill<T> {
  fn drop(&mut self) {
    self.0.drop_sender();
  }
}

pub struct OneshotGroup {
  history: Arc<WaitHistory>,
}
impl OneshotGroup {
  pub fn new() -> Self {
    Self {
      history: Arc::new(WaitHistory::new()),
    }
  }

  pub fn create_behavior<T>(&self) -> OneshotBehavior<T> {
    OneshotBehavior::new(self.history.clone())
  }

  /**
   * Creates a single-use channel pair (Oneshot, OneshotFulfill).
   * State transitions: Waiting → Fulfilled → Disconnected.
   * The receiver parks until the sender fulfills the value or disconnects.
   */
  pub fn create_pair<T>(&self) -> (Oneshot<T>, OneshotFulfill<T>) {
    let (p1, p2) = Pair::new(self.create_behavior());
    (Oneshot(p1), OneshotFulfill(p2))
  }
}
impl Clone for OneshotGroup {
  fn clone(&self) -> Self {
    Self {
      history: Arc::clone(&self.history),
    }
  }
}

#[cfg(test)]
#[path = "tests/oneshot.rs"]
mod tests;
