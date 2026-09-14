use std::{
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  thread::{park_timeout, Builder},
  time::Duration,
};

use super::{Close, SingleFn, ThreadSlot, UnwindSpawner};

const fn worker_loop(
  closed: Arc<AtomicBool>,
  mut work: SingleFn<'static, (), ()>,
  timeout: Duration,
) -> impl FnOnce() {
  move || {
    while !closed.load(Ordering::Acquire) {
      park_timeout(timeout);
      if closed.load(Ordering::Acquire) {
        return;
      }
      work.call(());
    }
  }
}

/**
 * Single-worker runtime with idle-time ticks.
 *
 * The timeout is not a precise periodic schedule. A tick means "no message has
 * arrived for at least this duration", so continuous explicit work can delay
 * ticks. This makes the runtime suitable for maintenance work that should run
 * during idle gaps.
 */
pub struct IntervalWorkThread {
  closed: Arc<AtomicBool>,
  slot: ThreadSlot,
}
impl IntervalWorkThread {
  pub fn new<S: ToString + Send + 'static>(
    name: S,
    size: usize,
    timeout: Duration,
    work: SingleFn<'static, (), ()>,
  ) -> Self {
    let closed = Arc::new(AtomicBool::new(false));
    let handle = Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn_unwind(worker_loop(closed.clone(), work, timeout));
    Self {
      closed,
      slot: ThreadSlot::new(handle),
    }
  }
}

impl Close for IntervalWorkThread {
  fn close(&self) {
    let Some(handle) = self.slot.close() else {
      return;
    };
    self.closed.fetch_or(true, Ordering::Release);
    handle.thread().unpark();
    handle.join().unwrap();
  }
}

#[cfg(test)]
#[path = "tests/interval.rs"]
mod tests;
