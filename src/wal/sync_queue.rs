use std::{
  io::Result as IOResult,
  sync::atomic::{AtomicU64, Ordering},
};

use crossbeam::{queue::SegQueue, utils::Backoff};

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

  /**
   * Wait until enough rotated segment fsyncs have completed.
   *
   * `generation` is used as a count-based durability barrier, not as an identity
   * lookup for a particular fsync result. When this returns successfully, at least
   * that many queued segment sync operations have completed.
   */
  pub fn wait_until(&self, generation: SegmentGeneration) -> IOResult<()> {
    let backoff = Backoff::new();
    while generation > self.synced_count.load(Ordering::Acquire) {
      let Some(fsync) = self.queue.pop() else {
        backoff.snooze();
        continue;
      };

      let result = fsync.wait().unwrap();
      // The counter tracks completed sync operations, not successful ones. A failed
      // fsync is still consumed from the queue; the error is returned to the caller to
      // handle WAL failure.
      self.synced_count.fetch_add(1, Ordering::Release);
      result?;
    }

    Ok(())
  }

  pub fn drain(&self) {
    while let Some(fsync) = self.queue.pop() {
      let _ = fsync.wait().unwrap();
      self.synced_count.fetch_add(1, Ordering::Release);
    }
  }
}
