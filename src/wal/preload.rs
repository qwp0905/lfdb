use std::{
  sync::Arc,
  thread::{park, park_timeout, Builder, Thread},
  time::{Duration, Instant},
};

use super::WALSegment;
use crate::{
  background::{oneshot, Oneshot, OneshotFulfill, ThreadSlot, UnwindSpawner},
  disk::{IOPool, Pointer},
  utils::{error, MpscQueue, ToBox},
  Result,
};

/**
 * Pre-allocates the next WAL segment in the background so rotation never blocks.
 * Reuses old segments via rename instead of creating new files.
 *
 * When idle leftover segments in
 * the reuse queue are truncated — no reason to hold pre-allocated disk space
 * when there is no burst traffic.
 */
pub struct SegmentPreload {
  preload: Box<PreloadThread>,
  io_pool: Arc<IOPool>,
  ready: Arc<MpscQueue<(Instant, WALSegment)>>,
}
impl SegmentPreload {
  pub fn new(
    max_len: Pointer,
    segment_expiration: Duration,
    segment_min_count: usize,
    io_pool: Arc<IOPool>,
  ) -> Self {
    let ready = Arc::new(MpscQueue::new());
    let preload = PreloadThread::new(
      ready.clone(),
      io_pool.clone(),
      max_len,
      segment_expiration,
      segment_min_count,
    )
    .to_box();
    Self {
      preload,
      ready,
      io_pool,
    }
  }

  pub fn load(&self) -> Result<WALSegment> {
    self.preload.request().wait().unwrap()
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
    while unsafe { self.ready.pop() }.is_some() {}
  }

  /**
   * must call after close segment rotate thread
   */
  pub fn close(&self) {
    self.preload.close();
    while let Some((_, segment)) = unsafe { self.ready.pop() } {
      let _ = segment.truncate();
    }
  }

  pub fn reuse(&self, reused: Vec<WALSegment>) {
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

    let now = Instant::now();
    for segment in succeeded {
      self.ready.push((now, segment));
    }
    self.preload.wake();
    for segment in failed {
      let _ = segment.truncate();
    }
  }
}

struct PreloadThread {
  queue: Arc<MpscQueue<Context>>,
  waker: Thread,
  slot: ThreadSlot,
}
impl PreloadThread {
  fn new(
    ready: Arc<MpscQueue<(Instant, WALSegment)>>,
    io_pool: Arc<IOPool>,
    max_len: Pointer,
    segment_expiration: Duration,
    segment_min_count: usize,
  ) -> Self {
    let queue = Arc::new(MpscQueue::new());
    let handle = Builder::new()
      .name("wal segment preload".to_string())
      .stack_size(2 << 20)
      .spawn_unwind(worker_loop(
        ready,
        queue.clone(),
        io_pool,
        max_len,
        segment_expiration,
        segment_min_count,
      ));
    Self {
      queue,
      waker: handle.thread().clone(),
      slot: ThreadSlot::new(handle),
    }
  }

  fn wake(&self) {
    self.waker.unpark();
  }

  fn request(&self) -> Oneshot<Result<WALSegment>> {
    let (o, f) = oneshot();
    self.queue.push(Context::Request(f));
    self.wake();
    o
  }

  fn close(&self) {
    let Some(handle) = self.slot.close() else {
      return;
    };
    self.queue.push(Context::Term);
    self.waker.unpark();
    handle.join().unwrap();
  }
}
impl Drop for PreloadThread {
  fn drop(&mut self) {
    self.close();
  }
}

enum Context {
  Request(OneshotFulfill<Result<WALSegment>>),
  Term,
}

fn create_new(io_pool: &IOPool, max_len: Pointer) -> Result<WALSegment> {
  let segment = WALSegment::open(max_len, io_pool)?;
  io_pool.sync_dir()?;
  Ok(segment)
}
const fn worker_loop(
  ready: Arc<MpscQueue<(Instant, WALSegment)>>,
  request: Arc<MpscQueue<Context>>,
  io_pool: Arc<IOPool>,
  max_len: Pointer,
  expiration: Duration,
  min_count: usize,
) -> impl FnOnce() {
  move || {
    let mut preloaded = None;
    let segment = loop {
      let (entered_at, segment) = preloaded
        .take()
        .or_else(|| unsafe { ready.pop() }.map(|(e, s)| (e, Ok(s))))
        .unwrap_or_else(|| (Instant::now(), create_new(&io_pool, max_len)));

      if let Some(ctx) = unsafe { request.pop() } {
        match ctx {
          Context::Request(done) => done.fulfill(segment),
          Context::Term => break segment,
        }
        continue;
      };

      if ready.len() < min_count {
        park();
        preloaded = Some((entered_at, segment));
        continue;
      }

      let elapsed = entered_at.elapsed();
      if elapsed < expiration {
        park_timeout(expiration - elapsed);
        preloaded = Some((entered_at, segment));
        continue;
      }

      let _ = segment.and_then(|seg| seg.truncate());
    };

    let _ = segment.and_then(|seg| seg.truncate());
  }
}
