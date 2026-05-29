use std::{sync::Arc, time::Duration};

use crossbeam::queue::SegQueue;

use crate::{
  cache::BlockCache,
  debug, error, info,
  thread::{BackgroundThread, WorkBuilder},
  transaction::VersionVisibility,
  utils::{ToArc, ToBox},
  wal::{WALSegment, WAL},
  Result,
};

pub struct Checkpoint {
  thread: Box<dyn BackgroundThread<WALSegment>>,
  failed: Arc<SegQueue<WALSegment>>,
}
impl Checkpoint {
  pub fn new(
    wal: Arc<WAL>,
    block_cache: Arc<BlockCache>,
    version_visibility: Arc<VersionVisibility>,
    interval: Duration,
    max_count: usize,
  ) -> Self {
    let failed = SegQueue::new().to_arc();
    let thread = WorkBuilder::new()
      .name("checkpoint")
      .single()
      .lazy_buffering(
        interval,
        max_count,
        handle_thread(wal, block_cache, version_visibility, failed.clone()),
      )
      .to_box();
    Self { thread, failed }
  }

  pub fn dispatch(&self, segment: WALSegment) {
    self.thread.dispatch(segment);
  }

  pub fn run(wal: &WAL, block_cache: &BlockCache, version: &VersionVisibility) -> Result {
    let log_id = wal.current_log_id();
    let current_version = version.current_version();
    info!("checkpoint trigger id {log_id} version {current_version}");

    block_cache.flush()?;
    let path = version.persist_snapshot(current_version)?;
    debug!("checkpoint snapshot persisted.");

    wal.checkpoint_and_flush(log_id, current_version, path.clone())?;
    info!("checkpoint complete id {log_id}");

    version.clear(&path)?;
    Ok(())
  }

  pub fn close(&self) {
    self.thread.close();
    while let Some(segment) = self.failed.pop() {
      segment.close();
    }
  }
}

const fn handle_thread(
  wal: Arc<WAL>,
  block_cache: Arc<BlockCache>,
  version: Arc<VersionVisibility>,
  failed: Arc<SegQueue<WALSegment>>,
) -> impl Fn(Vec<WALSegment>) {
  move |segments| {
    if let Err(err) = Checkpoint::run(&wal, &block_cache, &version) {
      error!("checkpoint failed: {err}");
      return segments.into_iter().for_each(|s| failed.push(s));
    }

    while let Some(buffered) = failed.pop() {
      wal.reuse(buffered);
    }
    segments.into_iter().for_each(|s| wal.reuse(s));
  }
}
