use std::{sync::Arc, time::Duration};

use crossbeam::queue::SegQueue;

use super::VersionVisibility;

use crate::{
  background::{BackgroundThread, EventBus, WorkBuilder},
  cache::BlockCache,
  debug,
  disk::IOPool,
  error, info,
  utils::ToArc,
  wal::{WALSegmentRotated, WAL},
  Result,
};

pub struct Checkpoint {
  thread: Arc<dyn BackgroundThread<WALSegmentRotated>>,
}
impl Checkpoint {
  pub fn new(
    wal: Arc<WAL>,
    block_cache: Arc<BlockCache>,
    version_visibility: Arc<VersionVisibility>,
    io_pool: Arc<IOPool>,
    event_bus: &EventBus,
    interval: Duration,
    max_count: usize,
  ) -> Self {
    let thread: Arc<dyn BackgroundThread<WALSegmentRotated>> = WorkBuilder::new()
      .name("checkpoint")
      .single()
      .lazy_buffering(
        interval,
        max_count,
        handle_thread(wal, block_cache, version_visibility, io_pool),
      )
      .to_arc();
    event_bus.register(&thread);
    Self { thread }
  }

  pub fn run(
    wal: &WAL,
    block_cache: &BlockCache,
    version: &VersionVisibility,
    io_pool: &IOPool,
  ) -> Result {
    let log_id = wal.current_log_id();
    let current_version = version.current_version();
    info!("checkpoint trigger id {log_id} version {current_version}");

    block_cache.flush()?;
    let path = version.persist_snapshot(current_version)?;
    debug!("checkpoint snapshot persisted.");
    io_pool.sync_dir()?;

    wal.checkpoint_and_flush(log_id, current_version, path.clone())?;
    info!("checkpoint complete id {log_id}");

    version.clear(&path)?;
    Ok(())
  }

  pub fn close(&self) {
    self.thread.close();
  }
}

const fn handle_thread(
  wal: Arc<WAL>,
  block_cache: Arc<BlockCache>,
  version: Arc<VersionVisibility>,
  io_pool: Arc<IOPool>,
) -> impl Fn(Vec<WALSegmentRotated>) {
  let failed = SegQueue::new();
  move |segments| {
    debug!("{} segment buffered for checkpoint.", segments.len());
    if let Err(err) = Checkpoint::run(&wal, &block_cache, &version, &io_pool) {
      error!("checkpoint failed: {err}");
      return segments.into_iter().for_each(|s| failed.push(s.0));
    }

    while let Some(buffered) = failed.pop() {
      wal.reuse(buffered);
    }
    segments.into_iter().for_each(|s| wal.reuse(s.0));
  }
}
