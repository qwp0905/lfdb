use std::{sync::Arc, time::Duration};

use crossbeam::queue::SegQueue;

use super::WALSegment;
use crate::{
  background::{BackgroundThread, WorkBuilder},
  disk::{IOPool, Pointer},
  error,
  utils::{ToArc, ToBox},
  Result,
};

const SEGMENT_MAX_LIFE: Duration = Duration::from_secs(5);
const SEGMENT_MAX_BATCH: usize = 10;

/**
 * Pre-allocates the next WAL segment in the background so rotation never blocks.
 * Reuses old segments via rename instead of creating new files.
 *
 * When idle (no rotation request within SEGMENT_MAX_LIFE), leftover segments in
 * the reuse queue are truncated — no reason to hold pre-allocated disk space
 * when there is no burst traffic.
 */
pub struct SegmentPreload {
  reuse: Box<dyn BackgroundThread<WALSegment, ()>>,
  preload: Box<dyn BackgroundThread<(), Result<WALSegment>>>,
  ready: Arc<SegQueue<WALSegment>>,
}
impl SegmentPreload {
  pub fn new(max_len: Pointer, io_pool: Arc<IOPool>) -> Self {
    let ready = SegQueue::new().to_arc();
    let reuse = WorkBuilder::new()
      .name("wal segment reuse")
      .single()
      .eager_buffering(
        SEGMENT_MAX_BATCH,
        handle_reuse(ready.clone(), io_pool.clone()),
      )
      .to_box();
    let preload = WorkBuilder::new()
      .name("wal segment preload")
      .single()
      .preload(
        SEGMENT_MAX_LIFE,
        handle_preload(ready.clone(), io_pool, max_len),
        handle_fallback(ready.clone()),
      )
      .to_box();
    Self {
      reuse,
      preload,
      ready,
    }
  }

  pub fn load(&self) -> Result<WALSegment> {
    self.preload.execute(()).wait().flatten()
  }

  /**
   * must call after close segment rotate thread
   */
  pub fn close(&self) {
    self.reuse.close();
    self.preload.close();
    while let Some(segment) = self.ready.pop() {
      let _ = segment.truncate();
    }
  }

  pub fn reuse(&self, segment: WALSegment) {
    self.reuse.dispatch(segment);
  }
}

fn handle_reuse(
  ready: Arc<SegQueue<WALSegment>>,
  io_pool: Arc<IOPool>,
) -> impl FnMut(Vec<WALSegment>) {
  let mut succeed = Vec::with_capacity(SEGMENT_MAX_BATCH);
  let mut failed = Vec::with_capacity(SEGMENT_MAX_BATCH);
  move |reused| {
    for segment in reused {
      if let Err(err) = segment.reuse() {
        error!("error occurs in segment reuse: {err}");
        failed.push(segment);
        continue;
      };
      succeed.push(segment);
    }
    if let Err(err) = io_pool.sync_dir() {
      error!("error occurs in basedir sync: {err}");
      succeed.drain(..).for_each(|s| failed.push(s));
    }

    for segment in succeed.drain(..) {
      ready.push(segment);
    }

    for segment in failed.drain(..) {
      let _ = segment.truncate();
    }
  }
}

const fn handle_preload(
  ready: Arc<SegQueue<WALSegment>>,
  io_pool: Arc<IOPool>,
  max_len: Pointer,
) -> impl FnMut(()) -> Result<WALSegment> {
  move |_| match ready.pop() {
    Some(segment) => Ok(segment),
    None => {
      WALSegment::open(max_len, &io_pool).and_then(|seg| io_pool.sync_dir().map(|_| seg))
    }
  }
}
const fn handle_fallback(
  ready: Arc<SegQueue<WALSegment>>,
) -> impl FnMut(Option<Result<WALSegment>>) {
  move |finalize| {
    if let Some(Ok(segment)) = finalize.or_else(|| ready.pop().map(Ok)) {
      let _ = segment.truncate();
    };
  }
}
