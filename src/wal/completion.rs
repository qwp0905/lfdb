use std::{
  cell::{Cell, UnsafeCell},
  collections::BTreeSet,
  io,
  sync::atomic::{AtomicU64, Ordering},
  thread::yield_now,
};

use crossbeam::{atomic::AtomicCell, queue::SegQueue, utils::Backoff};
use crossbeam_skiplist::SkipSet;

use super::{
  AppendOrder, AtomicLogId, BatchedWrite, FsyncResult, LogId, LogRecord,
  SegmentGeneration, WAL_BLOCK_SIZE,
};
use crate::{
  disk::Pointer,
  utils::{AtomicBitmap, AtomicSizedBitmap, BatchQueue},
};

const MAX_RECORD: usize = WAL_BLOCK_SIZE.div_ceil(LogRecord::MIN_BYTES);
const CAP: usize = AtomicSizedBitmap::<MAX_RECORD>::calc_capacity();

pub struct AppendCompletion {
  frontier: Cell<AppendOrder>,
  completed: AtomicSizedBitmap<CAP>,
}
impl AppendCompletion {
  pub const fn new(frontier: AppendOrder) -> Self {
    Self {
      frontier: Cell::new(frontier),
      completed: AtomicSizedBitmap::new(),
    }
  }
  pub fn wait_until(&self, threshold: AppendOrder) {
    let backoff = Backoff::new();
    loop {
      let ready = self.frontier.get();
      if ready >= threshold {
        break;
      }
      if !self.completed.contains(ready as u64) {
        backoff.snooze();
        continue;
      }
      self.frontier.set(ready + 1);
    }
  }

  pub fn complete(&self, order: AppendOrder) {
    self.completed.insert(order as u64);
  }
}

/**
 * A failed write is never marked completed, so the frontier cannot step over it.
 * Waiters do not know where their records landed, so once the completed prefix is
 * broken every later wait must fail.
 */
pub struct WriteCompletion {
  queue: SegQueue<(Pointer, BatchedWrite)>,
  frontier: AtomicU64,
  completed: AtomicBitmap,
  error: AtomicCell<Option<io::ErrorKind>>,
}
impl WriteCompletion {
  pub fn new(capacity: usize) -> Self {
    Self {
      queue: SegQueue::new(),
      frontier: AtomicU64::new(0),
      completed: AtomicBitmap::new(capacity),
      error: AtomicCell::new(None),
    }
  }

  pub fn error(&self) -> Option<io::ErrorKind> {
    self.error.load()
  }

  fn completed_from(&self, current: Pointer) -> impl Iterator<Item = Pointer> + '_ {
    (current..).take_while(|&i| self.completed.contains(i))
  }

  fn try_advance(&self, current: Pointer) {
    let Some(i) = self.completed_from(current).last() else {
      return;
    };
    self.frontier.fetch_max(i + 1, Ordering::Release);
  }

  pub fn wait_until(&self, threshold: Pointer) -> io::Result<()> {
    loop {
      if let Some(kind) = self.error.load() {
        return Err(kind.into());
      }
      let current = self.frontier.load(Ordering::Acquire);
      if threshold <= current {
        return Ok(());
      }
      let Some((ptr, batched)) = self.queue.pop() else {
        self.try_advance(current);
        yield_now();
        continue;
      };
      if let Err(err) = batched.wait() {
        let _ = self.error.compare_exchange(None, Some(err.kind()));
        return Err(err);
      }
      self.completed.insert(ptr);
      self.try_advance(current);
    }
  }

  pub fn register(&self, ptr: Pointer, batch: BatchedWrite) {
    self.queue.push((ptr, batch));
  }

  fn drain(&self) {
    while let Some((ptr, batched)) = self.queue.pop() {
      let _ = batched.wait();
      self.completed.insert(ptr);
      self.try_advance(self.frontier.load(Ordering::Acquire));
    }
  }
}
impl Drop for WriteCompletion {
  fn drop(&mut self) {
    self.drain();
  }
}

/**
 * Like `WriteCompletion`, a failure is kept instead of advancing the frontier, and
 * it covers every later generation because the prefix across segments is broken.
 */
pub struct SyncCompletion {
  queue: SegQueue<(SegmentGeneration, FsyncResult)>,
  frontier: AtomicU64,
  completed: SkipSet<SegmentGeneration>,
  error: AtomicCell<Option<io::ErrorKind>>,
}
impl SyncCompletion {
  pub fn new() -> Self {
    Self {
      queue: SegQueue::new(),
      frontier: AtomicU64::new(0),
      completed: SkipSet::new(),
      error: AtomicCell::new(None),
    }
  }

  pub fn register(&self, generation: SegmentGeneration, fsync: FsyncResult) {
    self.queue.push((generation, fsync));
  }

  pub fn failover(&self, kind: io::ErrorKind) {
    let _ = self.error.compare_exchange(None, Some(kind));
  }

  /**
   * Wait until enough rotated segment fsync has completed.
   *
   * `generation` is used as a count-based durability barrier, not as an identity
   * lookup for a particular fsync result. When this returns successfully, at least
   * that many queued segment sync operations have completed.
   */
  pub fn wait_until(&self, generation: SegmentGeneration) -> io::Result<()> {
    loop {
      if let Some(kind) = self.error.load() {
        return Err(kind.into());
      }
      let current = self.frontier.load(Ordering::Acquire);
      if generation <= current {
        return Ok(());
      }
      let Some((gen, pending)) = self.queue.pop() else {
        self.try_advance(current);
        yield_now();
        continue;
      };

      if let Err(err) = pending.wait() {
        let _ = self.error.compare_exchange(None, Some(err.kind()));
        return Err(err);
      }
      if current == gen {
        self.frontier.store(gen + 1, Ordering::Release);
      } else {
        self.completed.insert(gen);
      };
      self.try_advance(current);
    }
  }

  fn try_advance(&self, current: SegmentGeneration) {
    for i in (current..).take_while(|i| self.completed.remove(i).is_some()) {
      self.frontier.fetch_max(i + 1, Ordering::Release);
    }
  }

  pub fn drain(&self) {
    while let Some((gen, pending)) = self.queue.pop() {
      let _ = pending.wait();
      self.completed.insert(gen);
      for i in (self.frontier.load(Ordering::Acquire)..)
        .take_while(|i| self.completed.remove(i).is_some())
      {
        self.frontier.fetch_max(i + 1, Ordering::Release);
      }
    }
  }
}

/**
 * Only the batch owner advances the frontier, so completions never race on it
 * and the completed set needs no synchronization of its own.
 */
pub struct LogCompletion {
  frontier: AtomicLogId,
  batch: BatchQueue<LogId>,
  completed: UnsafeCell<BTreeSet<LogId>>,
}
impl LogCompletion {
  pub fn new(last_log_id: LogId) -> Self {
    Self {
      frontier: AtomicLogId::new(last_log_id),
      batch: BatchQueue::new(),
      completed: UnsafeCell::new(BTreeSet::new()),
    }
  }

  pub fn get_frontier(&self) -> LogId {
    self.frontier.load(Ordering::Acquire)
  }

  pub fn complete(&self, log_id: LogId) {
    if !self.batch.push_and_compete(log_id) {
      return;
    }

    loop {
      // Another owner may have advanced the frontier while ownership was released.
      let mut frontier = self.frontier.load(Ordering::Relaxed);
      let completed = unsafe { &mut *self.completed.get() };
      completed.extend(unsafe { self.batch.drain_all() });

      let advanced = completed
        .iter()
        .zip(frontier..)
        .take_while(|&(&id, expected)| id == expected)
        .count();
      if advanced > 0 {
        frontier += advanced as LogId;
        *completed = completed.split_off(&frontier);
        self.frontier.store(frontier, Ordering::Release);
      }

      if self.batch.try_release() {
        return;
      }
    }
  }
}
unsafe impl Sync for LogCompletion {}

#[cfg(all(test, not(loom)))]
#[path = "tests/completion.rs"]
mod tests;
