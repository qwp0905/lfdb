use std::{path::PathBuf, sync::Arc, time::Duration};

use crossbeam::{
  channel::{unbounded, Receiver},
  queue::SegQueue,
};

use super::{SegmentGeneration, WALSegment};
use crate::{
  disk::Pointer,
  thread::{BackgroundThread, WorkBuilder},
  utils::{ToArc, ToBox},
  Result,
};

const SEGMENT_MAX_LIFE: Duration = Duration::from_secs(5);

/**
 * Pre-allocates the next WAL segment in the background so rotation never blocks.
 * Reuses old segments via rename instead of creating new files.
 *
 * When idle (no rotation request within SEGMENT_MAX_LIFE), leftover segments in
 * the reuse queue are truncated — no reason to hold pre-allocated disk space
 * when there is no burst traffic.
 */
pub struct SegmentPreload {
  reuse: Arc<SegQueue<WALSegment>>,
  queue: Receiver<Result<WALSegment>>,
  thread: Box<dyn BackgroundThread<(), Result>>,
}
impl SegmentPreload {
  pub fn new(
    prefix: PathBuf,
    generation: SegmentGeneration,
    flush_count: usize,
    max_len: Pointer,
  ) -> Self {
    let (tx, rx) = unbounded();
    let reuse = SegQueue::<WALSegment>::new().to_arc();
    let reuse_c = reuse.clone();
    let mut generation = generation;
    let thread = WorkBuilder::new()
      .name("wal segment preloader")
      .single()
      .interval(SEGMENT_MAX_LIFE, move |trigger| {
        if trigger.is_none() {
          return reuse_c.pop().map(|seg| seg.truncate()).unwrap_or(Ok(()));
        }

        let current = generation;
        generation += 1;

        let segment = reuse_c
          .pop()
          .map(|seg| seg.reuse(&prefix, current).map(|_| seg))
          .unwrap_or_else(|| {
            WALSegment::open_new(&prefix, current, flush_count, max_len)
          })?;

        tx.send(Ok(segment)).unwrap();
        Ok(())
      })
      .to_box();

    let _ = thread.execute(());
    Self {
      queue: rx,
      thread,
      reuse,
    }
  }

  pub fn load(&self) -> Result<WALSegment> {
    let seg = self.queue.recv().unwrap();
    self.thread.execute(()).wait().flatten()?;
    seg
  }

  /**
   * must call after close segment rotate thread
   */
  pub fn close(&self) -> Result {
    self.thread.close();
    while let Ok(result) = self.queue.recv() {
      result.and_then(|seg| seg.truncate())?;
    }
    while let Some(seg) = self.reuse.pop() {
      seg.truncate()?;
    }
    Ok(())
  }

  pub fn reuse(&self, segment: WALSegment) {
    self.reuse.push(segment)
  }
}
unsafe impl Send for SegmentPreload {}
unsafe impl Sync for SegmentPreload {}
