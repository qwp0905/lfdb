use std::{
  io::Result as IOResult,
  sync::atomic::{AtomicU64, Ordering},
};

use crossbeam::{queue::SegQueue, utils::Backoff};

use crate::Result;

use super::{FsyncResult, SegmentGeneration};

/**
 * fsync results for rotated segments, pushed asynchronously at rotation time.
 * commit_and_flush drains this queue to ensure all prior segments are durable.
 * Without this, a commit written to segment N could be fsynced while segment N-1
 * (containing the corresponding insert) has not — losing data on crash.
 */
pub struct SyncQueue {
  queue: SegQueue<FsyncResult>,
  synced_count: AtomicU64,
}
impl SyncQueue {
  pub const fn new() -> Self {
    Self {
      queue: SegQueue::new(),
      synced_count: AtomicU64::new(0),
    }
  }

  pub fn push(&self, fsync: FsyncResult) {
    self.queue.push(fsync);
  }

  pub fn wait_until(&self, generation: SegmentGeneration) -> Result<IOResult<()>> {
    let backoff = Backoff::new();
    while generation > self.synced_count.load(Ordering::Acquire) {
      let Some(fsync) = self.queue.pop() else {
        backoff.snooze();
        continue;
      };

      let sync_r = fsync.wait()?;
      self.synced_count.fetch_add(1, Ordering::Release);
      if let Err(err) = sync_r {
        return Ok(Err(err));
      }
    }

    Ok(Ok(()))
  }

  pub fn drain(&self) {
    while let Some(fsync) = self.queue.pop() {
      let _ = fsync.wait();
      self.synced_count.fetch_add(1, Ordering::Release);
    }
  }
}
