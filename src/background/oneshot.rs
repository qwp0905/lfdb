use std::{
  cell::OnceCell, mem::MaybeUninit, ptr::without_provenance_mut, sync::atomic::Ordering,
};

use crate::utils::{
  loom::{
    thread::{current, Thread},
    thread_local, AtomicPtr, Backoff, UnsafeCell,
  },
  SBox,
};

pub enum TryWaitError<T> {
  Disconnected,
  Empty(T),
}
impl<T> TryWaitError<T> {
  pub fn map_empty<F, R>(self, project: F) -> TryWaitError<R>
  where
    F: FnOnce(T) -> R,
  {
    match self {
      Self::Disconnected => TryWaitError::Disconnected,
      Self::Empty(err) => TryWaitError::Empty(project(err)),
    }
  }
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

pub struct Oneshot<T> {
  state: Atomic<ThreadWaker>,
  value: UnsafeCell<MaybeUninit<T>>,
}
impl<T> Oneshot<T> {
  pub fn new() -> Self {
    Self {
      value: UnsafeCell::new(MaybeUninit::uninit()),
      state: Atomic::new(STATE_WAITING),
    }
  }

  pub unsafe fn fulfill_and_wake(&self, result: T) {
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

  pub unsafe fn try_wait(&self) -> std::result::Result<T, TryWaitError<()>> {
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

  pub unsafe fn try_park(&self) -> bool {
    let backoff = Backoff::new();
    let mut state = self.state.load();
    loop {
      match state {
        STATE_FULFILLED | STATE_DISCONNECTED => return false,
        STATE_WAITING => {
          let waker = CURRENT.with(|v| v.get_or_init(Default::default).clone());
          let raw = SBox::into_raw(waker);
          let Err(err) = self.state.cas_weak(state, raw) else {
            return true;
          };
          drop(unsafe { SBox::from_raw(raw) });
          backoff.spin();
          state = err;
        }
        _ => {
          let waker = CURRENT.with(|v| v.get_or_init(Default::default).clone());
          if state == waker.as_ptr() {
            return true;
          }
          let raw = SBox::into_raw(waker);
          let Err(err) = self.state.cas_weak(state, raw) else {
            drop(unsafe { SBox::from_raw(state) });
            return true;
          };
          drop(unsafe { SBox::from_raw(raw) });
          backoff.spin();
          state = err;
        }
      }
    }
  }

  pub unsafe fn drop_receiver(&self) {
    match self.state.swap(STATE_DISCONNECTED) {
      STATE_FULFILLED => unsafe { self.drop_value() },
      STATE_WAITING | STATE_DISCONNECTED => {}
      state => {
        let _ = unsafe { SBox::from_raw(state) };
      }
    }
  }

  pub unsafe fn drop_sender(&self) {
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

unsafe impl<T: Send> Sync for Oneshot<T> {}
unsafe impl<T: Send> Send for Oneshot<T> {}
