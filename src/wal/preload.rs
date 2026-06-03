use std::{path::PathBuf, sync::Arc, time::Duration};

use crossbeam::{
  channel::{unbounded, Receiver, Sender},
  queue::SegQueue,
};

use super::{SegmentGeneration, WALSegment};
use crate::{
  disk::{DirHandle, IOPool, Pointer},
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
    max_len: Pointer,
    io_pool: Arc<IOPool>,
    base_dir: Arc<DirHandle>,
  ) -> Self {
    let (tx, rx) = unbounded();
    let reuse = SegQueue::<WALSegment>::new().to_arc();
    let thread = WorkBuilder::new()
      .name("wal segment preloader")
      .single()
      .interval(
        SEGMENT_MAX_LIFE,
        handle_thread(
          prefix,
          reuse.clone(),
          generation,
          tx,
          max_len,
          io_pool,
          base_dir,
        ),
      )
      .to_box();

    let _ = thread.dispatch(());
    Self {
      queue: rx,
      thread,
      reuse,
    }
  }

  pub fn load(&self) -> Result<WALSegment> {
    let seg = self.queue.recv().unwrap();
    self.thread.dispatch(());
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

const fn handle_thread(
  prefix: PathBuf,
  reuse: Arc<SegQueue<WALSegment>>,
  mut generation: SegmentGeneration,
  ready: Sender<Result<WALSegment>>,
  max_len: Pointer,
  io_pool: Arc<IOPool>,
  base_dir: Arc<DirHandle>,
) -> impl FnMut(Option<()>) -> Result {
  move |trigger| {
    if trigger.is_none() {
      return reuse.pop().map(|seg| seg.truncate()).unwrap_or(Ok(()));
    }

    let current = generation;
    generation += 1;

    if let Some(segment) = reuse.pop() {
      segment.reuse(&prefix, current)?;
      ready.send(Ok(segment)).unwrap();
      return Ok(());
    }

    let segment = WALSegment::open(&prefix, current, max_len, &io_pool)?;
    base_dir.fdatasync()?;
    ready.send(Ok(segment)).unwrap();
    Ok(())
  }
}
