use super::*;

use std::{
  collections::VecDeque,
  sync::{Condvar, Mutex},
};

use crate::utils::ShortenedMutex;

struct State {
  generation: SegmentGeneration,
  queue: VecDeque<FsyncResult>,
}
pub struct SyncQueue {
  lock: Mutex<State>,
  cvar: Condvar,
}
impl SyncQueue {
  pub const fn new() -> Self {
    Self {
      lock: Mutex::new(State {
        generation: 0,
        queue: VecDeque::new(),
      }),
      cvar: Condvar::new(),
    }
  }

  pub fn push(&self, fsync: FsyncResult) {
    let mut state = self.lock.l();
    state.queue.push_back(fsync);
    self.cvar.notify_one();
  }

  pub fn wait_until(&self, generation: SegmentGeneration) -> Result<IOResult<()>> {
    let mut guard = self.lock.l();
    while guard.generation < generation {
      let Some(f) = guard.queue.pop_front() else {
        guard = self.cvar.wait(guard).unwrap();
        continue;
      };

      drop(guard);

      let result = f.wait()?;

      guard = self.lock.l();
      guard.generation += 1;
      self.cvar.notify_all();

      if let Err(err) = result {
        return Ok(Err(err));
      }
    }

    Ok(Ok(()))
  }

  pub fn drain(&self) {
    let mut guard = self.lock.l();
    while let Some(f) = guard.queue.pop_front() {
      let _ = f.wait();
      guard.generation += 1;
    }
    self.cvar.notify_all();
  }
}
