use std::{
  cell::UnsafeCell,
  io::Result,
  ptr::null_mut,
  sync::{
    atomic::{AtomicPtr, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use crossbeam::queue::SegQueue;

use super::{PendingSegment, WALSegment};
use crate::{
  disk::{IOPool, Pointer},
  utils::{error, ChunkQueue, MpscQueue},
};

/**
 * Supplies the next WAL segment to rotation.
 *
 * Idle reused segments are released — no reason to hold pre-allocated disk
 * space when there is no burst traffic.
 */
pub struct SegmentPreload {
  ready: SegQueue<WALSegment>,
  /**
   * Always holds a raw `PendingSegment` until `close`, so a new segment is
   * prepared one rotation ahead instead of on demand.
   */
  slot: AtomicPtr<()>,
  /**
   * Kept apart so cleanup I/O for failed creations runs in the maintenance loop,
   * never on the rotating thread.
   */
  failed: MpscQueue<WALSegment>,
  /**
   * Only `reuse` pushes into `ready` and it records every push here. Since `ready`
   * is FIFO and its segments are interchangeable, the surplus over `ready.len()`
   * is exactly the stale front.
   */
  expiry: UnsafeCell<ChunkQueue<Instant>>,
  io_pool: Arc<IOPool>,
  max_len: Pointer,
  expiration: Duration,
  min_count: usize,
}
impl SegmentPreload {
  pub fn new(
    max_len: Pointer,
    segment_expiration: Duration,
    segment_min_count: usize,
    io_pool: Arc<IOPool>,
  ) -> Self {
    let pending = WALSegment::open(max_len, &io_pool);
    Self {
      ready: SegQueue::new(),
      slot: AtomicPtr::new(PendingSegment::into_raw(pending)),
      failed: MpscQueue::new(),
      expiry: UnsafeCell::new(ChunkQueue::new()),
      io_pool,
      max_len,
      expiration: segment_expiration,
      min_count: segment_min_count,
    }
  }

  pub fn load(&self) -> Result<WALSegment> {
    if let Some(segment) = self.ready.pop() {
      return Ok(segment);
    }

    let next = PendingSegment::into_raw(WALSegment::open(self.max_len, &self.io_pool));
    let prev = self.slot.swap(next, Ordering::AcqRel);
    // SAFETY: the swap hands the previous raw `PendingSegment` to this caller alone.
    let pending = unsafe { PendingSegment::from_raw(prev) };
    let (err, segment) = match pending.resolve() {
      Ok(segment) => return Ok(segment),
      Err(failed) => failed,
    };
    if let Some(segment) = segment {
      self.failed.push(segment);
    }
    Err(err)
  }

  /**
   * must call after the maintenance loop has stopped
   */
  pub fn close(&self) {
    while let Some(segment) = self.ready.pop() {
      let _ = segment.truncate();
    }
    // SAFETY: with the maintenance loop stopped, this is the only consumer.
    while let Some(segment) = unsafe { self.failed.pop() } {
      let _ = segment.truncate();
    }

    let raw = self.slot.swap(null_mut(), Ordering::AcqRel);
    if raw.is_null() {
      return;
    }
    // SAFETY: a non-null slot holds a raw `PendingSegment`, now owned here.
    let segment = match unsafe { PendingSegment::from_raw(raw) }.resolve() {
      Ok(segment) => segment,
      Err((_, Some(segment))) => segment,
      Err((_, None)) => return,
    };
    let _ = segment.truncate();
  }

  /**
   * # Safety
   * Must be called from a single thread together with `expire`, since both mutate
   * the expiry log without synchronization.
   */
  pub unsafe fn reuse(&self, reused: Vec<WALSegment>) {
    let mut succeeded = Vec::with_capacity(reused.len());
    let mut failed = Vec::new();
    for segment in reused {
      if segment.reuse().is_ok() {
        succeeded.push(segment);
      } else {
        failed.push(segment);
      };
    }
    if succeeded.is_empty() {
      return;
    }
    if let Err(err) = self.io_pool.sync_dir() {
      error!("wal segment reuse failed: {err}");
      for segment in succeeded.into_iter().chain(failed) {
        let _ = segment.truncate();
      }
      return;
    }

    // SAFETY: the caller confines `reuse` and `expire` to one thread.
    let expiry = unsafe { &mut *self.expiry.get() };
    let now = Instant::now();
    for segment in succeeded {
      expiry.push(now);
      self.ready.push(segment);
    }
    for segment in failed {
      let _ = segment.truncate();
    }
  }

  /**
   * # Safety
   * Must be called from a single thread together with `reuse`, since both mutate
   * the expiry log without synchronization.
   */
  pub unsafe fn expire(&self) {
    // SAFETY: `expire` runs on one thread and `close` only after it stops, so this is
    // the only consumer.
    while let Some(segment) = unsafe { self.failed.pop() } {
      let _ = segment.truncate();
    }

    // SAFETY: the caller confines `reuse` and `expire` to one thread.
    let expiry = unsafe { &mut *self.expiry.get() };
    loop {
      for _ in self.ready.len()..expiry.len() {
        expiry.pop();
      }
      if expiry.len() <= self.min_count {
        return;
      }
      if expiry
        .pop_if(|entered_at| entered_at.elapsed() >= self.expiration)
        .is_none()
      {
        return;
      }
      let Some(segment) = self.ready.pop() else {
        return;
      };
      let _ = segment.truncate();
    }
  }
}
impl Drop for SegmentPreload {
  fn drop(&mut self) {
    let raw = *self.slot.get_mut();
    if raw.is_null() {
      return;
    }
    // SAFETY: a non-null slot holds a raw `PendingSegment`, now owned here.
    drop(unsafe { PendingSegment::from_raw(raw) });
  }
}

// SAFETY: the expiry log is the only unsynchronized state, and the contracts of
// `reuse` and `expire` confine every access to it to one thread.
unsafe impl Sync for SegmentPreload {}
