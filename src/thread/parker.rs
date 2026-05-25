#[cfg(not(target_os = "linux"))]
mod cvar {
  use std::sync::{Condvar, Mutex};

  use crate::utils::ShortenedMutex;

  struct State {
    locked: bool,
    waiters: usize,
  }
  pub struct OnceParker {
    lock: Mutex<State>,
    cvar: Condvar,
  }

  impl OnceParker {
    pub const fn new() -> Self {
      Self {
        lock: Mutex::new(State {
          locked: false,
          waiters: 0,
        }),
        cvar: Condvar::new(),
      }
    }

    pub fn park(&self) {
      let mut guard = self.lock.l();
      while !guard.locked {
        guard.waiters += 1;
        guard = self.cvar.wait(guard).unwrap();
        guard.waiters -= 1;
      }
    }

    pub fn wake_all(&self) {
      {
        let mut guard = self.lock.l();
        guard.locked = true;
        if guard.waiters == 0 {
          return;
        }
      }
      self.cvar.notify_all();
    }
  }
}

#[cfg(target_os = "linux")]
mod futex {
  use std::{
    ptr::null,
    sync::atomic::{AtomicBool, AtomicU32, Ordering},
  };

  const PARKED: u32 = 0;
  const WAKEN: u32 = 1;
  pub struct OnceParker {
    state: AtomicU32,
    waiters: AtomicBool,
  }
  impl OnceParker {
    pub const fn new() -> Self {
      Self {
        state: AtomicU32::new(PARKED),
        waiters: AtomicBool::new(false),
      }
    }
    pub fn park(&self) {
      if self.state.load(Ordering::Acquire) == WAKEN {
        return;
      }

      self.waiters.fetch_or(true, Ordering::Release);
      while self.state.load(Ordering::Acquire) == PARKED {
        unsafe {
          libc::syscall(
            libc::SYS_futex,
            self.state.as_ptr(),
            libc::FUTEX_WAIT_BITSET | libc::FUTEX_PRIVATE_FLAG,
            PARKED,
            null::<libc::timespec>(),
          )
        };
      }
    }

    pub fn wake_all(&self) {
      self.state.store(WAKEN, Ordering::Release);

      if !self.waiters.load(Ordering::Acquire) {
        return;
      }

      unsafe {
        libc::syscall(
          libc::SYS_futex,
          self.state.as_ptr(),
          libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG,
          i32::MAX,
        )
      };
    }
  }
}

#[cfg(not(target_os = "linux"))]
pub use cvar::*;

#[cfg(target_os = "linux")]
pub use futex::*;
