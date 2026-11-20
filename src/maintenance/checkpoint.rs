use std::{
  cell::UnsafeCell,
  mem::take,
  path::PathBuf,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::Instant,
};

use super::CheckpointSnapshot;

use crate::{
  background::{binding_events, EventBus, OwnedSubscription, SharedSubscription},
  blob::BlobStorage,
  cache::{BlockCache, CacheFlusher},
  disk::IOPool,
  metrics::MetricsRegistry,
  mvcc::VersionController,
  page::PAGE_SIZE,
  utils::{debug, error, info, trace, uuid_simple, MpscQueue},
  wal::{WALFailed, WALSegment, WALSegmentRotated, WriteAheadLog, WriteSeq},
  Error, Result,
};

pub struct CheckpointConfig {
  pub flush_factor: f64,
}

const BATCH_SIZE: f64 = ((1 << 20) / PAGE_SIZE / 2) as f64; // convert from mib/sec

struct CheckpointCycle {
  segments: Vec<WALSegment>,
  flusher: CacheFlusher,
  write_seq: WriteSeq,
  start: Option<Instant>,
}
impl CheckpointCycle {
  fn new(
    segments: Vec<WALSegment>,
    flusher: CacheFlusher,
    write_seq: WriteSeq,
    start: Option<Instant>,
  ) -> Self {
    Self {
      segments,
      flusher,
      write_seq,
      start,
    }
  }
  fn flush_hard(&mut self) -> Result {
    self.flusher.flush_hard()
  }
  fn finish_flush(&self) -> Result {
    self.flusher.finish()
  }
  fn advance_flush(&mut self, count: usize) -> Result {
    self.flusher.advance(count)
  }
  fn flush_done(&self) -> bool {
    self.flusher.is_done()
  }
  fn truncate_all(self) -> Result {
    for segment in self.segments {
      segment.truncate()?;
    }
    Ok(())
  }
  fn take_segments(&mut self) -> Vec<WALSegment> {
    take(&mut self.segments)
  }
  const fn get_write_seq(&self) -> WriteSeq {
    self.write_seq
  }
  const fn segments_len(&self) -> usize {
    self.segments.len()
  }
  fn dirty_len(&self) -> usize {
    self.flusher.remaining()
  }
  const fn take_start(&mut self) -> Option<Instant> {
    self.start.take()
  }
  fn is_empty(&self) -> bool {
    self.segments.is_empty() && self.flusher.is_done()
  }
}

pub struct Checkpoint {
  incoming: MpscQueue<WALSegmentRotated>,
  /**
   * Shared storage for the active checkpoint cycle.
   *
   * The interval worker is the only mutator while running, but shutdown must take
   * over the current checkpoint context after closing the worker and finish it
   * synchronously.
   */
  cycle: UnsafeCell<Option<CheckpointCycle>>,
  worker: CheckpointWorker,
  failed: AtomicBool,
  config: CheckpointConfig,
}
impl Checkpoint {
  fn with_worker(
    worker: CheckpointWorker,
    event_bus: Arc<EventBus>,
    config: CheckpointConfig,
  ) -> Arc<Self> {
    let incoming = MpscQueue::new();
    let this = Arc::new(Self {
      incoming,
      cycle: UnsafeCell::new(None),
      worker,
      failed: AtomicBool::new(false),
      config,
    });
    event_bus.register(&this);
    this
  }
  pub fn new(
    wal: Arc<WriteAheadLog>,
    block_cache: Arc<BlockCache>,
    version_controller: Arc<VersionController>,
    io_pool: Arc<IOPool>,
    blob_storage: Arc<BlobStorage>,
    event_bus: Arc<EventBus>,
    metrics: Arc<MetricsRegistry>,
    config: CheckpointConfig,
  ) -> Arc<Self> {
    let worker = CheckpointWorker::new(
      wal,
      block_cache,
      version_controller,
      io_pool,
      blob_storage,
      metrics,
    );
    Self::with_worker(worker, event_bus, config)
  }

  pub fn run_manually(&self) -> Result {
    self.worker.run_hard()?;
    Ok(())
  }

  pub unsafe fn run_tick(&self) {
    if self.failed.load(Ordering::Relaxed) {
      return;
    }
    let Err(err) = self.worker.run_tick(
      &self.incoming,
      unsafe { &mut *self.cycle.get() },
      self.config.flush_factor,
    ) else {
      return;
    };
    if !matches!(err, Error::IO(_) | Error::WALFailed(_)) {
      panic!("{err}");
    }
    error!("checkpoint failed: {err}");
  }

  /**
   * Reduce replay work during normal shutdown.
   *
   * If all rotated segments are already part of the active checkpoint cycle, close
   * only finishes that cycle. If new rotated segments are still waiting in
   * `incoming`, they would remain replay candidates, so close runs one final hard
   * checkpoint and retires them as well.
   */
  pub fn close(&self) -> Result {
    if !self.worker.wal.is_available() {
      return Ok(());
    }
    if self.failed.fetch_or(true, Ordering::Relaxed) {
      return Ok(());
    }
    if self.incoming.is_empty() {
      let Some(mut cycle) = unsafe { &mut *self.cycle.get() }.take() else {
        return Ok(());
      };

      cycle.flush_hard()?;
      self.worker.finalize_checkpoint(cycle.get_write_seq())?;
      cycle.truncate_all()?;
      info!("last checkpoint completed.");
      return Ok(());
    }

    let seq = self.worker.run_hard()?;
    while let Some(event) = unsafe { self.incoming.pop() } {
      if seq >= event.last_write_seq {
        event.segment.truncate()?;
      }
    }

    if let Some(cycle) = unsafe { &mut *self.cycle.get() }.take() {
      cycle.truncate_all()?;
    }

    info!("last checkpoint completed.");
    Ok(())
  }
}
// SAFETY: `cycle` is touched only by `run_tick`, which the event loop calls from
// its own thread, and by `close`, which runs after the loop has stopped.
unsafe impl Sync for Checkpoint {}

impl OwnedSubscription<WALSegmentRotated> for Checkpoint {
  fn handle(&self, event: WALSegmentRotated) {
    self.incoming.push(event)
  }
}
impl SharedSubscription<WALFailed> for Checkpoint {
  fn handle(&self, _: Arc<WALFailed>) {
    error!("checkpoint stopped since wal failure detected.");
    self.failed.store(true, Ordering::Relaxed);
  }
}
binding_events!(Checkpoint {
  owned: [WALSegmentRotated],
  shared: [WALFailed]
});

const FILE_EXT: &str = "snap";

struct CheckpointWorker {
  wal: Arc<WriteAheadLog>,
  block_cache: Arc<BlockCache>,
  version_controller: Arc<VersionController>,
  io_pool: Arc<IOPool>,
  blob_storage: Arc<BlobStorage>,
  metrics: Arc<MetricsRegistry>,
}
impl CheckpointWorker {
  const fn new(
    wal: Arc<WriteAheadLog>,
    block_cache: Arc<BlockCache>,
    version_controller: Arc<VersionController>,
    io_pool: Arc<IOPool>,
    blob_storage: Arc<BlobStorage>,
    metrics: Arc<MetricsRegistry>,
  ) -> Self {
    Self {
      wal,
      block_cache,
      version_controller,
      io_pool,
      blob_storage,
      metrics,
    }
  }

  fn finalize_checkpoint(&self, write_seq: WriteSeq) -> Result {
    let (current_version, active, aborted) = self.version_controller.snapshot();
    let blobs = self.blob_storage.metadata_snapshot();
    let snapshot = CheckpointSnapshot::new(active, aborted, blobs);

    let current = PathBuf::from(uuid_simple()).with_extension(FILE_EXT);
    snapshot.write_at(&mut self.io_pool.open_append_io(&current)?)?;
    debug!("checkpoint snapshot persisted.");
    self.io_pool.sync_dir()?;

    self
      .wal
      .checkpoint_and_flush(write_seq, current_version, current.clone())?;

    for entry in self.io_pool.read_dir()? {
      let name = PathBuf::from(entry.file_name());
      if name.extension().is_none_or(|ext| ext != FILE_EXT) {
        continue;
      };
      if name == current {
        continue;
      }
      self.io_pool.truncate(&name)?;
    }
    Ok(())
  }

  fn run_hard(&self) -> Result<WriteSeq> {
    let write_seq = self.wal.completed_write_seq();
    info!("hard checkpoint trigger id {write_seq}.");

    self.block_cache.create_flusher(write_seq)?.flush_hard()?;
    self.finalize_checkpoint(write_seq)?;
    info!("hard checkpoint complete id {write_seq}");
    Ok(write_seq)
  }

  fn create_cycle(
    &self,
    incoming: &MpscQueue<WALSegmentRotated>,
  ) -> Result<CheckpointCycle> {
    let write_seq = self.wal.completed_write_seq();
    let flusher = self.block_cache.create_flusher(write_seq)?;
    let mut non_durable = Vec::new();
    let mut durable = Vec::new();
    while let Some(event) = unsafe { incoming.pop() } {
      if write_seq >= event.last_write_seq {
        durable.push(event.segment);
      } else {
        non_durable.push(event);
      }
    }
    for event in non_durable {
      incoming.push(event);
    }
    Ok(CheckpointCycle::new(
      durable,
      flusher,
      write_seq,
      self.metrics.checkpoint_cycle.start(),
    ))
  }

  fn run_tick(
    &self,
    incoming: &MpscQueue<WALSegmentRotated>,
    cycle: &mut Option<CheckpointCycle>,
    flush_factor: f64,
  ) -> Result {
    unsafe { self.wal.expire_segments() };

    let Some(current) = cycle else {
      let new = self.create_cycle(incoming)?;
      if new.is_empty() {
        debug!("no checkpoint required.");
        return Ok(());
      }

      info!(
        "new checkpoint cycle created for id {}, dirty blocks {}, segments {}",
        new.get_write_seq(),
        new.dirty_len(),
        new.segments_len(),
      );
      *cycle = Some(new);
      return Ok(());
    };

    if !current.flush_done() {
      // Increase checkpoint work as WAL rotation pressure grows.
      // The number of retired-but-not-yet-reusable WAL segments is treated as write
      // pressure. Higher pressure flushes more dirty blocks per tick, both throttling
      // ongoing write load indirectly and moving segment reuse forward sooner.
      let batch_size = (flush_factor.powi((current.segments_len() + incoming.len()) as _)
        * BATCH_SIZE) as usize;
      trace!("checkpoint flush {} blocks", batch_size);
      return current.advance_flush(batch_size);
    }

    info!(
      "checkpoint id {} trying to finish.",
      current.get_write_seq()
    );

    current.finish_flush()?;
    debug!("block cache all flushed.");

    if current.segments_len() == 0 {
      debug!("skip create checkpoint snapshot since nothing to rotate.");
      self.metrics.checkpoint_cycle.record(current.take_start());
      *cycle = None;
      return Ok(());
    }

    self.finalize_checkpoint(current.get_write_seq())?;
    self.metrics.checkpoint_cycle.record(current.take_start());
    info!("checkpoint complete id {}", current.get_write_seq());

    unsafe { self.wal.reuse_segments(current.take_segments()) };
    *cycle = None;
    Ok(())
  }
}
