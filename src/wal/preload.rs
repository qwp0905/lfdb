use std::{path::PathBuf, sync::Arc, time::Duration};

use crossbeam::queue::ArrayQueue;

use super::WALSegment;
use crate::{
  disk::{DirHandle, IOPool, Pointer},
  error,
  thread::{BackgroundThread, WorkBuilder},
  utils::{ToArc, ToBox},
  Result,
};

const SEGMENT_MAX_LIFE: Duration = Duration::from_secs(5);
const SEGMENT_MAX_BATCH: usize = 3;

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
  ready: Arc<ArrayQueue<WALSegment>>,
}
impl SegmentPreload {
  pub fn new(
    prefix: PathBuf,
    max_len: Pointer,
    io_pool: Arc<IOPool>,
    base_dir: Arc<DirHandle>,
  ) -> Self {
    let ready = ArrayQueue::new(SEGMENT_MAX_BATCH).to_arc();
    let reuse = WorkBuilder::new()
      .name("wal segment reuse")
      .single()
      .eager_buffering(
        SEGMENT_MAX_BATCH,
        handle_reuse(ready.clone(), base_dir.clone(), prefix.clone()),
      )
      .to_box();
    let preload = WorkBuilder::new()
      .name("wal segment preload")
      .single()
      .preload(
        SEGMENT_MAX_LIFE,
        handle_preload(ready.clone(), prefix, io_pool, base_dir, max_len),
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
  ready: Arc<ArrayQueue<WALSegment>>,
  base_dir: Arc<DirHandle>,
  prefix: PathBuf,
) -> impl FnMut(Vec<WALSegment>) {
  let mut succeed = Vec::with_capacity(SEGMENT_MAX_BATCH);
  let mut failed = Vec::with_capacity(SEGMENT_MAX_BATCH);
  move |reused| {
    for segment in reused {
      if ready.len() + succeed.len() >= SEGMENT_MAX_BATCH {
        failed.push(segment);
        continue;
      }

      if let Err(err) = segment.reuse(&prefix) {
        error!("error occurs in segment reuse: {err}");
        failed.push(segment);
        continue;
      };
      succeed.push(segment);
    }
    if let Err(err) = base_dir.fdatasync() {
      error!("error occurs in basedir sync: {err}");
      succeed.drain(..).for_each(|s| failed.push(s));
    }

    for segment in succeed
      .drain(..)
      .flat_map(|s| ready.push(s).err())
      .chain(failed.drain(..))
    {
      let _ = segment.truncate();
    }
  }
}

const fn handle_preload(
  ready: Arc<ArrayQueue<WALSegment>>,
  prefix: PathBuf,
  io_pool: Arc<IOPool>,
  base_dir: Arc<DirHandle>,
  max_len: Pointer,
) -> impl FnMut(()) -> Result<WALSegment> {
  move |_| match ready.pop() {
    Some(segment) => Ok(segment),
    None => WALSegment::open(&prefix, max_len, &io_pool)
      .and_then(|seg| base_dir.fdatasync().map(|_| seg)),
  }
}
const fn handle_fallback(
  ready: Arc<ArrayQueue<WALSegment>>,
) -> impl FnMut(Option<Result<WALSegment>>) {
  move |finalize| {
    if let Some(Ok(segment)) = finalize.or_else(|| ready.pop().map(Ok)) {
      let _ = segment.truncate();
    };
  }
}
