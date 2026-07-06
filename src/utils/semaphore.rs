#[cfg(not(target_os = "linux"))]
mod fallback {
  use std::sync::{Condvar, Mutex};

  struct State {
    waiting: usize,
    permits: usize,
  }
  pub struct Semaphore {
    state: Mutex<State>,
    cvar: Condvar,
  }
  impl Semaphore {
    pub const fn new(permits: usize) -> Self {
      Self {
        state: Mutex::new(State {
          waiting: 0,
          permits,
        }),
        cvar: Condvar::new(),
      }
    }

    pub fn acquire(&self) -> Permit<'_> {
      let mut state = self.state.lock().unwrap();
      while state.permits == 0 {
        state.waiting += 1;
        state = self.cvar.wait(state).unwrap();
        state.waiting -= 1;
      }
      state.permits -= 1;
      Permit(self)
    }

    fn release(&self) {
      let mut state = self.state.lock().unwrap();
      state.permits += 1;
      if state.waiting > 0 {
        self.cvar.notify_one();
      }
    }
  }

  pub struct Permit<'a>(&'a Semaphore);
  impl<'a> Drop for Permit<'a> {
    fn drop(&mut self) {
      self.0.release();
    }
  }
}

#[cfg(target_os = "linux")]
mod futex {
  use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

  fn wait(futex: &AtomicU32, expected: u32) {
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

  fn wake_one(futex: &AtomicU32) {
    unsafe {
      libc::syscall(
        libc::SYS_futex,
        futex.as_ptr(),
        libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG,
        1i32,
      );
    };
  }

  // fn wake_all(futex: &AtomicU32) {
  //   unsafe {
  //     libc::syscall(
  //       libc::SYS_futex,
  //       futex.as_ptr(),
  //       libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG,
  //       libc::INT_MAX,
  //     );
  //   };
  // }

  pub struct Semaphore {
    permits: AtomicU32,
    waiting: AtomicUsize,
  }
  impl Semaphore {
    pub const fn new(permits: usize) -> Self {
      Self {
        permits: AtomicU32::new(permits),
        waiting: AtomicUsize::new(0),
      }
    }

    pub fn acquire(&self) -> Permit<'_> {
      loop {
        let n = self.permits.load(Ordering::Acquire);
        if n > 0
          && self
            .permits
            .compare_exchange_weak(n, n - 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
          return Permit(self);
        }

        self.waiting.fetch_add(1, Ordering::Release);
        while self.permits.load(Ordering::Acquire) == 0 {
          wait(&self.permits, 0);
        }
        self.waiting.fetch_sub(1, Ordering::Release);
      }
    }

    fn release(&self) {
      self.permits.fetch_add(1, Ordering::Release);
      if self.waiters.load(Ordering::Acquire) > 0 {
        wake_one(&self.permits);
      }
    }
  }

  pub struct Permit<'a>(&'a Semaphore);
  impl<'a> Drop for Permit<'a> {
    fn drop(&mut self) {
      self.0.release();
    }
  }
}

#[cfg(not(target_os = "linux"))]
pub use fallback::*;
#[cfg(target_os = "linux")]
pub use futex::*;
