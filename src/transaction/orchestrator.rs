use std::sync::Arc;

use super::{FreeList, PageRecorder, VersionVisibility};

use crate::{
  buffer_pool::{BufferPool, BufferPoolConfig, Slot, WritableSlot},
  cursor::{GarbageCollectionConfig, GarbageCollector},
  error::Result,
  serialize::Serializable,
  thread::{BackgroundThread, WorkBuilder, WorkInput},
  utils::{LogFilter, ToArc, ToBox},
  wal::{WALConfig, WAL},
};

pub struct TxOrchestrator {
  wal: Arc<WAL>,
  buffer_pool: Arc<BufferPool>,
  checkpoint: Box<dyn BackgroundThread<(), Result>>,
  free_list: Arc<FreeList>,
  version_visibility: Arc<VersionVisibility>,
  gc: Arc<GarbageCollector>,
  recorder: Arc<PageRecorder>,
  logger: LogFilter,
}
impl TxOrchestrator {
  pub fn new(
    buffer_pool_config: BufferPoolConfig,
    wal_config: WALConfig,
    gc_config: GarbageCollectionConfig,
    logger: LogFilter,
  ) -> Result<(Self, bool)> {
    let buffer_pool = BufferPool::open(buffer_pool_config, logger.clone())?.to_arc();
    let checkpoint_interval = wal_config.checkpoint_interval;
    let checkpoint_ch = WorkInput::new();

    let (wal, replay) = WAL::replay(wal_config, checkpoint_ch.copy(), logger.clone())?;
    let wal = wal.to_arc();
    let recorder = PageRecorder::new(wal.clone()).to_arc();
    for (_, i, data) in replay.redo {
      buffer_pool
        .read(i)?
        .for_write()
        .as_mut()
        .writer()
        .write(data.as_ref())?;
    }

    buffer_pool.flush()?;
    let disk_len = buffer_pool.disk_len()?;
    let free_list = FreeList::new(disk_len).to_arc();

    let version_visibility =
      VersionVisibility::new(replay.aborted, replay.last_tx_id).to_arc();

    let gc = GarbageCollector::start(
      buffer_pool.clone(),
      version_visibility.clone(),
      free_list.clone(),
      recorder.clone(),
      logger.clone(),
      gc_config,
    )
    .to_arc();

    if !replay.is_new {
      gc.release_orphand(disk_len)?;
      logger.info("orphand block has released successfully.");
      run_checkpoint(&wal, &buffer_pool, &gc, &version_visibility, &logger)?;
      replay
        .segments
        .into_iter()
        .map(|seg| seg.truncate())
        .collect::<Result>()?;
    }
    gc.ready();

    let checkpoint = WorkBuilder::new()
      .name("checkpoint")
      .stack_size(2 << 20)
      .single()
      .from_channel(checkpoint_ch)
      .interval(
        checkpoint_interval,
        handle_checkpoint(
          wal.clone(),
          buffer_pool.clone(),
          gc.clone(),
          version_visibility.clone(),
          logger.clone(),
        ),
      )?
      .to_box();

    Ok((
      Self {
        checkpoint,
        wal,
        free_list,
        buffer_pool,
        version_visibility,
        gc,
        recorder,
        logger,
      },
      replay.is_new,
    ))
  }
  pub fn fetch(&self, index: usize) -> Result<Slot<'_>> {
    self.buffer_pool.read(index)
  }

  pub fn serialize_and_log<T>(
    &self,
    tx_id: usize,
    slot: &mut WritableSlot<'_>,
    data: &T,
  ) -> Result
  where
    T: Serializable,
  {
    self.recorder.serialize_and_log(tx_id, slot, data)
  }

  pub fn alloc(&self) -> Result<WritableSlot<'_>> {
    let free = self.free_list.alloc();
    Ok(self.buffer_pool.read(free)?.for_write())
  }

  pub fn mark_gc(&self, index: usize) {
    self.gc.mark(index);
  }

  pub fn start_tx(&self) -> Result<usize> {
    let tx_id = self.version_visibility.new_transaction();
    self.wal.append_start(tx_id)?;
    Ok(tx_id)
  }

  pub fn commit_tx(&self, tx_id: usize) -> Result {
    self.wal.commit_and_flush(tx_id)?;
    self.version_visibility.deactive(&tx_id);
    Ok(())
  }

  pub fn abort_tx(&self, tx_id: usize) -> Result {
    self.wal.append_abort(tx_id)?;
    self.version_visibility.move_to_abort(tx_id);
    Ok(())
  }

  pub fn is_visible(&self, tx_id: &usize) -> bool {
    self.version_visibility.is_visible(tx_id)
  }

  pub fn is_active(&self, tx_id: &usize) -> bool {
    self.version_visibility.is_active(tx_id)
  }

  pub fn current_version(&self) -> usize {
    self.version_visibility.current_version()
  }

  pub fn close(&self) -> Result {
    let wal_close = self.wal.twostep_close();
    self.checkpoint.close();
    self.logger.info("last checkpoint completed.");

    self.gc.close();
    self.buffer_pool.close();
    self.logger.info("buffer pool closed.");
    wal_close();
    self.logger.info("wal closed.");
    Ok(())
  }
}

fn handle_checkpoint(
  wal: Arc<WAL>,
  buffer_pool: Arc<BufferPool>,
  gc: Arc<GarbageCollector>,
  version: Arc<VersionVisibility>,
  logger: LogFilter,
) -> impl Fn(Option<()>) -> Result {
  move |_| run_checkpoint(&wal, &buffer_pool, &gc, &version, &logger)
}

fn run_checkpoint(
  wal: &WAL,
  buffer_pool: &BufferPool,
  gc: &GarbageCollector,
  version: &VersionVisibility,
  logger: &LogFilter,
) -> Result {
  let log_id = wal.current_log_id();
  let min_version = version.min_version();
  logger.debug(format!(
    "checkpoint trigger id {log_id} version {min_version}"
  ));

  gc.run()?;
  version.remove_aborted(&min_version);
  logger.debug(format!("checkpoint garbage collected id {log_id}"));

  buffer_pool.flush()?;
  wal.checkpoint_and_flush(log_id, min_version)?;
  logger.debug(format!("checkpoint complete id {log_id}"));
  Ok(())
}
