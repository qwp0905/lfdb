use std::{fs::File, sync::Arc, time::Duration};

use crossbeam::queue::SegQueue;

use crate::{
  cache::BlockCache,
  debug, error, info,
  thread::{BackgroundThread, WorkBuilder},
  transaction::VersionVisibility,
  utils::ToBox,
  wal::{WALSegment, WAL},
  Error, Result,
};

pub struct Checkpoint {
  thread: Box<dyn BackgroundThread<WALSegment>>,
}
impl Checkpoint {
  pub fn new(
    wal: Arc<WAL>,
    block_cache: Arc<BlockCache>,
    version_visibility: Arc<VersionVisibility>,
    base_dir: File,
    interval: Duration,
    max_count: usize,
  ) -> Self {
    let thread = WorkBuilder::new()
      .name("checkpoint")
      .single()
      .lazy_buffering(
        interval,
        max_count,
        handle_thread(wal, block_cache, version_visibility, base_dir),
      )
      .to_box();
    Self { thread }
  }

  pub fn dispatch(&self, segment: WALSegment) {
    self.thread.dispatch(segment);
  }

  pub fn run(
    wal: &WAL,
    block_cache: &BlockCache,
    version: &VersionVisibility,
    base_dir: &File,
  ) -> Result {
    let log_id = wal.current_log_id();
    let current_version = version.current_version();
    info!("checkpoint trigger id {log_id} version {current_version}");

    block_cache.flush()?;
    let path = version.persist_snapshot(current_version)?;
    debug!("checkpoint snapshot persisted.");
    base_dir.sync_data().map_err(Error::IO)?;

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
  base_dir: File,
) -> impl Fn(Vec<WALSegment>) {
  let failed = SegQueue::new();
  move |segments| {
    if let Err(err) = Checkpoint::run(&wal, &block_cache, &version, &base_dir) {
      error!("checkpoint failed: {err}");
      return segments.into_iter().for_each(|s| failed.push(s));
    }

    while let Some(buffered) = failed.pop() {
      wal.reuse(buffered);
    }
    segments.into_iter().for_each(|s| wal.reuse(s));
  }
}
