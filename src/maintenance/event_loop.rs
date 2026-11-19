use std::{
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  thread::{park_timeout, Builder},
  time::{Duration, Instant},
};

use crate::{
  background::{EventBus, EventRouter, ThreadSlot, UnwindSpawner},
  utils::error,
  Result,
};

use super::{Checkpoint, Compactor, GarbageCollector};

pub struct EventLoop {
  closed: Arc<AtomicBool>,
  thread: ThreadSlot<Result>,
}
impl EventLoop {
  pub fn new(
    event_bus: Arc<EventBus>,
    checkpoint: Arc<Checkpoint>,
    garbage_collector: Arc<GarbageCollector>,
    compactor: Arc<Compactor>,
  ) -> Self {
    let closed = Arc::new(AtomicBool::new(false));
    let handle = Builder::new()
      .name("event loop".to_string())
      .stack_size(2 << 20)
      .spawn_unwind(create_event_loop(
        event_bus,
        checkpoint,
        garbage_collector,
        compactor,
        closed.clone(),
      ));
    Self {
      closed,
      thread: ThreadSlot::new(handle),
    }
  }
  pub fn close(&self) {
    let Some(handle) = self.thread.close() else {
      return;
    };

    self.closed.fetch_or(true, Ordering::Relaxed);
    handle.thread().unpark();
    if let Err(err) = handle.join().unwrap() {
      error!("event loop closed with error: {err}");
    }
  }
}

const TICK_SIZE: Duration = Duration::from_millis(500);

const fn create_event_loop(
  event_bus: Arc<EventBus>,
  checkpoint: Arc<Checkpoint>,
  garbage_collector: Arc<GarbageCollector>,
  compactor: Arc<Compactor>,
  closed: Arc<AtomicBool>,
) -> impl FnOnce() -> Result {
  move || {
    let mut event_router = EventRouter::new(event_bus);
    let mut last = Instant::now();
    loop {
      unsafe { event_router.run_tick() };
      if closed.load(Ordering::Relaxed) {
        break;
      }

      let now = Instant::now();
      if now - last >= TICK_SIZE {
        last = now;

        unsafe { checkpoint.run_tick() };
        if closed.load(Ordering::Relaxed) {
          break;
        }
        unsafe { garbage_collector.run_tick() };
        if closed.load(Ordering::Relaxed) {
          break;
        }
        unsafe { compactor.run_tick() };
      }

      if closed.load(Ordering::Relaxed) {
        break;
      }
      if let Some(dur) = (last + TICK_SIZE).checked_duration_since(Instant::now()) {
        park_timeout(dur);
      }
    }

    compactor.close()?;
    garbage_collector.close();
    checkpoint.close()
  }
}
