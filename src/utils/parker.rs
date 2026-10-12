#[cfg(not(target_os = "linux"))]
mod fallback {
  use std::sync::{Condvar, Mutex};

  struct State {
    generation: u32,
    waiters: usize,
  }
  impl State {
    const fn new() -> Self {
      Self {
        generation: 0,
        waiters: 0,
      }
    }
  }
  pub struct ThreadParker {
    state: Mutex<State>,
    cvar: Condvar,
  }
  impl ThreadParker {
    pub const fn new() -> Self {
      Self {
        state: Mutex::new(State::new()),
        cvar: Condvar::new(),
      }
    }

    pub fn park_or_cancel<T>(&self, has_next: impl FnOnce() -> Option<T>) -> Option<T> {
      let generation = {
        let state = self.state.lock().unwrap();
        state.generation
      };
      if let Some(next) = has_next() {
        return Some(next);
      };

      let mut state = self.state.lock().unwrap();
      while state.generation == generation {
        state.waiters += 1;
        state = self.cvar.wait(state).unwrap();
        state.waiters -= 1;
      }
      None
    }

    pub fn wake_one(&self) {
      {
        let mut state = self.state.lock().unwrap();
        state.generation += 1;
        if state.waiters == 0 {
          return;
        }
      }
      self.cvar.notify_one();
    }

    pub fn wake_all(&self) {
      {
        let mut state = self.state.lock().unwrap();
        state.generation += 1;
        if state.waiters == 0 {
          return;
        }
      }
      self.cvar.notify_all();
    }
  }
}

#[cfg(target_os = "linux")]
mod futex {
  use std::sync::atomic::{fence, AtomicU32, AtomicUsize, Ordering};

  fn futex_wait(futex: &AtomicU32, expected: u32) {
    unsafe {
      libc::syscall(
        libc::SYS_futex,
        futex.as_ptr(),
        libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG,
        expected as libc::c_int,
        std::ptr::null::<libc::timespec>(),
      );
    };
  }

  fn futex_wake_one(futex: &AtomicU32) {
    unsafe {
      libc::syscall(
        libc::SYS_futex,
        futex.as_ptr(),
        libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG,
        1i32,
      );
    };
  }

  fn futex_wake_all(futex: &AtomicU32) {
    unsafe {
      libc::syscall(
        libc::SYS_futex,
        futex.as_ptr(),
        libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG,
        libc::INT_MAX,
      );
    };
  }

  pub struct ThreadParker {
    generation: AtomicU32,
    waiters: AtomicUsize,
  }
  impl ThreadParker {
    pub const fn new() -> Self {
      Self {
        generation: AtomicU32::new(0),
        waiters: AtomicUsize::new(0),
      }
    }

    pub fn park_or_cancel<T>(&self, has_next: impl FnOnce() -> Option<T>) -> Option<T> {
      let generation = self.generation.load(Ordering::Acquire);
      if let Some(next) = has_next() {
        return Some(next);
      };

      self.waiters.fetch_add(1, Ordering::Relaxed);
      fence(Ordering::SeqCst);
      while self.generation.load(Ordering::Acquire) == generation {
        futex_wait(&self.generation, generation);
      }
      self.waiters.fetch_sub(1, Ordering::Relaxed);
      None
    }

    pub fn wake_one(&self) {
      self.generation.fetch_add(1, Ordering::Release);
      fence(Ordering::SeqCst);

      if self.waiters.load(Ordering::Relaxed) > 0 {
        futex_wake_one(&self.generation)
      }
    }
    pub fn wake_all(&self) {
      self.generation.fetch_add(1, Ordering::Release);
      fence(Ordering::SeqCst);

      if self.waiters.load(Ordering::Relaxed) > 0 {
        futex_wake_all(&self.generation)
      }
    }
  }
}

use std::sync::Once;

#[cfg(not(target_os = "linux"))]
use fallback as inner;
#[cfg(target_os = "linux")]
use futex as inner;

pub struct ThreadParker(inner::ThreadParker);
impl ThreadParker {
  pub const fn new() -> Self {
    Self(inner::ThreadParker::new())
  }
  pub fn park_or_cancel<T>(&self, has_next: impl FnOnce() -> Option<T>) -> Option<T> {
    self.0.park_or_cancel(has_next)
  }
  pub fn wake_once(&self) {
    self.0.wake_one();
  }
  pub fn wake_all(&self) {
    self.0.wake_all();
  }
}

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
