use std::{
  io::Result as IOResult,
  sync::atomic::{AtomicU64, Ordering},
};

use crossbeam::{queue::SegQueue, utils::Backoff};
use crossbeam_skiplist::SkipSet;

use super::{FsyncResult, SegmentGeneration};

/**
 * fsync results for rotated segments, pushed asynchronously at rotation time.
 * commit_and_flush drains this queue to ensure all prior segments are durable.
 * Without this, a commit written to segment N could be fsynced while segment N-1
 * (containing the corresponding insert) has not — losing data on crash.
 */
pub struct SyncQueue {
  queue: SegQueue<(SegmentGeneration, FsyncResult)>,
  frontier: AtomicU64,
  buffered: SkipSet<SegmentGeneration>,
}
impl SyncQueue {
  pub fn new() -> Self {
    Self {
      queue: SegQueue::new(),
      frontier: AtomicU64::new(0),
      buffered: SkipSet::new(),
    }
  }

  pub fn push(&self, generation: SegmentGeneration, fsync: FsyncResult) {
    self.queue.push((generation, fsync));
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
    loop {
      let current = self.frontier.load(Ordering::Acquire);
      if generation <= current {
        return Ok(());
      }
      let Some((gen, pending)) = self.queue.pop() else {
        self.try_advance(current);
        backoff.snooze();
        continue;
      };

      let result = pending.wait();
      self.buffered.insert(gen);
      self.try_advance(current);
      result?;
    }
  }

  fn try_advance(&self, current: SegmentGeneration) {
    for i in (current..).take_while(|i| self.buffered.remove(i).is_some()) {
      self.frontier.fetch_max(i + 1, Ordering::Release);
    }
  }

  pub fn drain(&self) {
    while let Some((gen, pending)) = self.queue.pop() {
      let _ = pending.wait();
      self.buffered.insert(gen);
      for i in (self.frontier.load(Ordering::Acquire)..)
        .take_while(|i| self.buffered.remove(i).is_some())
      {
        self.frontier.fetch_max(i + 1, Ordering::Release);
      }
    }
  }
}
