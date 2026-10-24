use std::sync::{Condvar, Mutex};

struct State {
  waiting: usize,
  permits: u32,
}

pub struct Semaphore {
  state: Mutex<State>,
  cvar: Condvar,
}
impl Semaphore {
  pub const fn new(permits: u32) -> Self {
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

#[cfg(all(test, not(loom)))]
#[path = "tests/semaphore.rs"]
mod tests;
