use std::{
  cell::OnceCell, mem::MaybeUninit, ptr::without_provenance_mut, sync::atomic::Ordering,
};

use crate::utils::{
  loom::{
    thread::{current, park, yield_now, Thread},
    thread_local, AtomicPtr, Backoff, UnsafeCell,
  },
  SBox,
};

pub enum TryWaitError<T> {
  Disconnected,
  Empty(T),
}
#[derive(Debug)]
pub struct WaitDisconnectedError;

// A waker pointer stored in `state` owns one `SBox` count; whoever swaps it out
// reclaims that count with `SBox::from_raw`.
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
impl Default for ThreadWaker {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(not(loom))]
thread_local! {
  static CURRENT: OnceCell<SBox<ThreadWaker>> = const { OnceCell::new() };
}
// Loom's thread_local does not accept a const initializer.
#[cfg(loom)]
thread_local! {
  static CURRENT: OnceCell<SBox<ThreadWaker>> = OnceCell::new();
}

// Tagged pointers
const STATE_WAITING: *mut ThreadWaker = without_provenance_mut(0);
const STATE_FULFILLED: *mut ThreadWaker = without_provenance_mut(1);
const STATE_DISCONNECTED: *mut ThreadWaker = without_provenance_mut(2);

struct Atomic<T>(AtomicPtr<T>);
impl<T> Atomic<T> {
  fn new(ptr: *mut T) -> Self {
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

pub struct OneshotBehavior<T> {
  state: Atomic<ThreadWaker>,
  value: UnsafeCell<MaybeUninit<T>>,
}
impl<T> OneshotBehavior<T> {
  pub fn new() -> Self {
    Self {
      value: UnsafeCell::new(MaybeUninit::uninit()),
      state: Atomic::new(STATE_WAITING),
    }
  }

  pub fn fulfill_and_wake(&self, result: T) {
    self.value.with_mut(|v| unsafe { (*v).write(result) });
    let backoff = Backoff::new();
    let mut state = self.state.load();
    loop {
      if state == STATE_DISCONNECTED {
        return unsafe { self.drop_value() };
      }

      if let Err(err) = self.state.cas_weak(state, STATE_FULFILLED) {
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

  unsafe fn read_value(&self) -> T {
    self.value.with(|v| unsafe { (*v).assume_init_read() })
  }
  unsafe fn drop_value(&self) {
    self.value.with_mut(|v| unsafe { (*v).assume_init_drop() });
  }

  fn try_park(&self) -> Result<bool, WaitDisconnectedError> {
    CURRENT.with(|v| {
      let waker = v.get_or_init(Default::default);
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
            let raw = SBox::into_raw(waker.clone());
            let Err(err) = self.state.cas_weak(state, raw) else {
              return Ok(true);
            };
            drop(unsafe { SBox::from_raw(raw) });
            backoff.spin();
            state = err;
          }
          _ => return Ok(true),
        }
      }
    })
  }

  #[cfg(not(loom))]
  const MAX_YIELD: u8 = 10;
  // Loom keeps scheduling the other thread while one yields, so a long yield
  // phase lets the sender always finish before the waiter reaches park.
  #[cfg(loom)]
  const MAX_YIELD: u8 = 1;
  pub fn wait(&self) -> Result<T, WaitDisconnectedError> {
    loop {
      for _ in 0..Self::MAX_YIELD {
        match self.try_wait() {
          Ok(v) => return Ok(v),
          Err(TryWaitError::Disconnected) => return Err(WaitDisconnectedError),
          Err(TryWaitError::Empty(_)) => yield_now(),
        };
      }
      if !self.try_park()? {
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

#[cfg(all(test, loom))]
#[path = "tests/oneshot_loom.rs"]
mod tests;
