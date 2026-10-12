use std::sync::atomic::{AtomicU32, Ordering};

use crossbeam::utils::Backoff;

use super::ThreadParker;

pub struct Semaphore {
  permits: AtomicU32,
  parker: ThreadParker,
}
impl Semaphore {
  pub const fn new(permits: u32) -> Self {
    Self {
      permits: AtomicU32::new(permits),
      parker: ThreadParker::new(),
    }
  }

  fn try_acquire(&self) -> bool {
    let n = self.permits.load(Ordering::Acquire);
    if n == 0 {
      return false;
    }
    if self
      .permits
      .compare_exchange(n, n - 1, Ordering::Acquire, Ordering::Relaxed)
      .is_err()
    {
      return false;
    }

    true
  }

  fn release(&self) {
    self.permits.fetch_add(1, Ordering::Release);
    self.parker.wake_once();
  }

  pub fn acquire(&self) -> Permit<'_> {
    let backoff = Backoff::new();
    loop {
      while !backoff.is_completed() {
        if self.try_acquire() {
          return Permit(self);
        }
        backoff.snooze();
      }
      if self
        .parker
        .park_or_cancel(|| self.try_acquire().then_some(()))
        .is_some()
      {
        return Permit(self);
      };
      backoff.reset();
    }
  }
}

pub struct Permit<'a>(&'a Semaphore);
impl<'a> Drop for Permit<'a> {
  fn drop(&mut self) {
    self.0.release();
  }
}

#[cfg(test)]
#[path = "tests/semaphore.rs"]
mod tests;
