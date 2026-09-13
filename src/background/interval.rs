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
  move || loop {
    park_timeout(timeout);
    if closed.load(Ordering::Acquire) {
      return;
    }
    work.call(());
    if closed.load(Ordering::Acquire) {
      return;
    }
  }
}

/**
 * Single-worker runtime with idle-time ticks.
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
    self.closed.store(true, Ordering::Release);
    handle.thread().unpark();
    handle.join().unwrap();
  }
}

#[cfg(test)]
#[path = "tests/interval.rs"]
mod tests;
