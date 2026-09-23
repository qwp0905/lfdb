use std::sync::Arc;

use crossbeam::queue::SegQueue;

use super::WALSegment;
use crate::{
  background::{Close, Execute, PreloadThread, ThreadBuilder},
  disk::{IOPool, Pointer},
  utils::ToBox,
  Result,
};

/**
 * Pre-allocates the next WAL segment in the background so rotation never blocks.
 * Reuses old segments via rename instead of creating new files.
 *
 * When idle (no rotation request within SEGMENT_MAX_LIFE), leftover segments in
 * the reuse queue are truncated — no reason to hold pre-allocated disk space
 * when there is no burst traffic.
 */
pub struct SegmentPreload {
  preload: Box<PreloadThread<Result<WALSegment>>>,
  io_pool: Arc<IOPool>,
  ready: SegQueue<WALSegment>,
}
impl SegmentPreload {
  pub fn new(max_len: Pointer, io_pool: Arc<IOPool>) -> Self {
    let ready = SegQueue::new();
    let preload = ThreadBuilder::new()
      .name("wal segment preload")
      .single()
      .preload(handle_preload(io_pool.clone(), max_len), handle_fallback())
      .to_box();
    Self {
      preload,
      ready,
      io_pool,
    }
  }

  pub fn load(&self) -> Result<WALSegment> {
    if let Some(segment) = self.ready.pop() {
      return Ok(segment);
    }
    self.preload.execute(()).wait().unwrap()
  }

  /**
   * Stop without cleanup I/O after WAL failure.
   *
   * In failover state the engine can no longer trust WAL I/O, so the preloader
   * only stops workers and drops queued handles. Normal shutdown can still issue
   * cleanup truncates, but failover must not depend on more disk operations.
   */
  pub fn failover(&self) {
    self.preload.close();
    while self.ready.pop().is_some() {}
  }

  /**
   * must call after close segment rotate thread
   */
  pub fn close(&self) {
    self.preload.close();
    while let Some(segment) = self.ready.pop() {
      let _ = segment.truncate();
    }
  }

  pub fn reuse(&self, reused: Vec<WALSegment>) -> Result {
    reused.iter().try_for_each(|seg| seg.reuse())?;
    self.io_pool.sync_dir()?;
    for segment in reused {
      self.ready.push(segment);
    }
    Ok(())
  }
}

const fn handle_preload(
  io_pool: Arc<IOPool>,
  max_len: Pointer,
) -> impl FnMut(()) -> Result<WALSegment> {
  move |_| {
    let segment = WALSegment::open(max_len, &io_pool)?;
    io_pool.sync_dir()?;
    Ok(segment)
  }
}

/**
 * Drop an unused preloaded segment when segment demand is low.
 *
 * The preload fallback runs when no caller consumed the prepared segment within
 * the idle window. That implies low WAL write pressure, so keeping preallocated
 * disk space is unnecessary; the segment is truncated instead.
 */
const fn handle_fallback() -> impl FnMut(Result<WALSegment>) {
  move |finalize| {
    if let Ok(segment) = finalize {
      let _ = segment.truncate();
    };
  }
}
