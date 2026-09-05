use std::{
  cell::Cell,
  io,
  sync::atomic::{AtomicU64, Ordering},
  thread::yield_now,
};

use crossbeam::{queue::SegQueue, utils::Backoff};
use crossbeam_skiplist::SkipSet;

use super::{BatchedWrite, FsyncResult, LogRecord, SegmentGeneration, WAL_BLOCK_SIZE};
use crate::{
  disk::Pointer,
  utils::{AtomicBitmap, AtomicSizedBitmap},
};

const MAX_RECORD: usize = WAL_BLOCK_SIZE.div_ceil(LogRecord::MIN_BYTES);
const CAP: usize = AtomicSizedBitmap::<MAX_RECORD>::calc_capacity();

// pub struct CompletionGate(Semaphore);
// impl CompletionGate {
//   pub fn new(count: u32) -> Self {
//     Self(Semaphore::new(count))
//   }
// }

pub struct AppendCompletion {
  frontier: Cell<u32>,
  completed: AtomicSizedBitmap<CAP>,
}
impl AppendCompletion {
  pub const fn new(frontier: u32) -> Self {
    Self {
      frontier: Cell::new(frontier),
      completed: AtomicSizedBitmap::new(),
    }
  }
  pub fn wait_until(&self, threshold: u32) {
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

  pub fn complete(&self, order: u32) {
    self.completed.insert(order as u64);
  }
}

pub struct WriteCompletion {
  queue: SegQueue<(Pointer, BatchedWrite)>,
  frontier: AtomicU64,
  completed: AtomicBitmap,
}
impl WriteCompletion {
  pub fn new(capacity: usize) -> Self {
    Self {
      queue: SegQueue::new(),
      frontier: AtomicU64::new(0),
      completed: AtomicBitmap::new(capacity),
    }
  }

  fn take_from(&self, current: Pointer) -> impl Iterator<Item = Pointer> + '_ {
    (current..).take_while(|&i| self.completed.contains(i))
  }

  pub fn wait_until(&self, threshold: Pointer) -> io::Result<()> {
    loop {
      let current = self.frontier.load(Ordering::Acquire);
      if threshold <= current {
        return Ok(());
      }
      let Some((ptr, batched)) = self.queue.pop() else {
        yield_now();
        continue;
      };
      let result = batched.wait();
      self.completed.insert(ptr);
      if let Some(i) = self.take_from(current).last() {
        self.frontier.fetch_max(i + 1, Ordering::Release);
      }
      result?;
    }
  }

  pub fn register(&self, ptr: Pointer, batch: BatchedWrite) {
    self.queue.push((ptr, batch));
  }

  pub fn drain(&self) {
    while let Some((ptr, batched)) = self.queue.pop() {
      let _ = batched.wait();
      self.completed.insert(ptr);

      let current = self.frontier.load(Ordering::Acquire);
      if let Some(i) = self.take_from(current).last() {
        self.frontier.fetch_max(i + 1, Ordering::Release);
      }
    }
  }
}

pub struct SyncCompletion {
  queue: SegQueue<(SegmentGeneration, FsyncResult)>,
  frontier: AtomicU64,
  completed: SkipSet<SegmentGeneration>,
}
impl SyncCompletion {
  pub fn new() -> Self {
    Self {
      queue: SegQueue::new(),
      frontier: AtomicU64::new(0),
      completed: SkipSet::new(),
    }
  }

  pub fn register(&self, generation: SegmentGeneration, fsync: FsyncResult) {
    self.queue.push((generation, fsync));
  }

  /**
   * Wait until enough rotated segment fsyncs have completed.
   *
   * `generation` is used as a count-based durability barrier, not as an identity
   * lookup for a particular fsync result. When this returns successfully, at least
   * that many queued segment sync operations have completed.
   */
  pub fn wait_until(&self, generation: SegmentGeneration) -> io::Result<()> {
    loop {
      let current = self.frontier.load(Ordering::Acquire);
      if generation <= current {
        return Ok(());
      }
      let Some((gen, pending)) = self.queue.pop() else {
        self.try_advance(current);
        yield_now();
        continue;
      };

      let result = pending.wait();
      self.completed.insert(gen);
      self.try_advance(current);
      result?;
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
