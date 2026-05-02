use std::sync::{Condvar, Mutex};

use crate::utils::ShortenedMutex;

struct State {
  locked: bool,
  high_waiters: usize,
}

pub struct Latch {
  state: Mutex<State>,
  high_cv: Condvar,
  low_cv: Condvar,
}
impl Latch {
  pub const fn new() -> Self {
    Self {
      state: Mutex::new(State {
        locked: false,
        high_waiters: 0,
      }),
      high_cv: Condvar::new(),
      low_cv: Condvar::new(),
    }
  }

  pub fn lock_immediately<'a, 'b>(&'a self) -> LatchGuard<'b>
  where
    'a: 'b,
  {
    let mut state = self.state.l();
    while state.locked {
      state.high_waiters += 1;
      state = self.high_cv.wait(state).unwrap();
      state.high_waiters -= 1;
    }
    state.locked = true;

    LatchGuard(self)
  }

  pub fn lock_lazily<'a, 'b>(&'a self) -> LatchGuard<'b>
  where
    'a: 'b,
  {
    let mut state = self.state.l();
    while state.locked || state.high_waiters > 0 {
      state = self.low_cv.wait(state).unwrap();
    }
    state.locked = true;

    LatchGuard(self)
  }

  fn unlock(&self) {
    let mut state = self.state.l();
    state.locked = false;
    if state.high_waiters > 0 {
      self.high_cv.notify_one();
    } else {
      self.low_cv.notify_one();
    }
  }
}

pub struct LatchGuard<'a>(&'a Latch);
impl<'a> Drop for LatchGuard<'a> {
  fn drop(&mut self) {
    self.0.unlock();
  }
}

#[cfg(test)]
#[path = "tests/latch.rs"]
mod tests;
