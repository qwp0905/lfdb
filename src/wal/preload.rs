use std::{sync::Arc, time::Duration};

use crossbeam::queue::SegQueue;

use super::WALSegment;
use crate::{
  background::{BackgroundThread, EventBus, OwnedSubscription, ThreadBuilder},
  binding_events,
  disk::{IOPool, Pointer},
  error,
  utils::{ToArc, ToBox},
  Result,
};

const SEGMENT_MAX_LIFE: Duration = Duration::from_secs(5);
const SEGMENT_MAX_BATCH: usize = 10;

pub struct SegmentReuseable(WALSegment);
impl SegmentReuseable {
  pub const fn new(segment: WALSegment) -> Self {
    Self(segment)
  }
}

/**
 * Pre-allocates the next WAL segment in the background so rotation never blocks.
 * Reuses old segments via rename instead of creating new files.
 *
 * When idle (no rotation request within SEGMENT_MAX_LIFE), leftover segments in
 * the reuse queue are truncated — no reason to hold pre-allocated disk space
 * when there is no burst traffic.
 */
pub struct SegmentPreload {
  reuse: Arc<dyn BackgroundThread<WALSegment, ()>>,
  preload: Box<dyn BackgroundThread<(), Result<WALSegment>>>,
  ready: Arc<SegQueue<WALSegment>>,
}
impl SegmentPreload {
  pub fn new(max_len: Pointer, io_pool: Arc<IOPool>, event_bus: &EventBus) -> Arc<Self> {
    let ready = SegQueue::new().to_arc();
    let reuse = ThreadBuilder::new()
      .name("wal segment reuse")
      .single()
      .buffering(
        SEGMENT_MAX_BATCH,
        handle_reuse(ready.clone(), io_pool.clone()),
      )
      .to_arc();
    let preload = ThreadBuilder::new()
      .name("wal segment preload")
      .single()
      .preload(
        SEGMENT_MAX_LIFE,
        handle_preload(ready.clone(), io_pool, max_len),
        handle_fallback(),
      )
      .to_box();
    let this = Arc::new(Self {
      reuse,
      preload,
      ready,
    });

    event_bus.register(&this);
    this
  }

  pub fn load(&self) -> Result<WALSegment> {
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
    self.reuse.close();
    self.preload.close();
    while self.ready.pop().is_some() {}
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

impl OwnedSubscription<SegmentReuseable> for SegmentPreload {
  fn handle(&self, event: SegmentReuseable) {
    self.reuse(event.0);
  }
}
binding_events!(SegmentPreload {
  owned: [SegmentReuseable],
});

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
  move |_| {
    if let Some(segment) = ready.pop() {
      return Ok(segment);
    }
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
const fn handle_fallback() -> impl FnMut(Option<Result<WALSegment>>) {
  move |finalize| {
    if let Some(Ok(segment)) = finalize {
      let _ = segment.truncate();
    };
  }
}
