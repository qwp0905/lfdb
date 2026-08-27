use std::{
  iter::repeat,
  path::PathBuf,
  sync::Arc,
  time::{Duration, Instant},
};

use crossbeam::{atomic::AtomicCell, queue::SegQueue};

use super::{CheckpointSnapshot, VersionVisibility};

use crate::{
  background::{
    Close, EventBus, IntervalWorkThread, OwnedSubscription, SharedSubscription,
    ThreadBuilder,
  },
  binding_events,
  blob::BlobStorage,
  cache::{BlockCache, CacheFlusher},
  debug,
  disk::{IOPool, PAGE_SIZE},
  error, info,
  metrics::MetricsRegistry,
  trace,
  utils::{uuid_simple, ToArc, ToBox},
  wal::{
    LogId, SegmentReuseable, WALFailed, WALSegment, WALSegmentRotated, WriteAheadLog,
  },
  Result,
};

const CHECKPOINT_TICK: Duration = Duration::from_millis(500);
const BATCH_SIZE: f64 = ((1 << 20) / PAGE_SIZE / 2) as f64; // convert from mib/sec

struct CheckpointCycle {
  segments: Vec<WALSegment>,
  flusher: CacheFlusher,
  log_id: LogId,
  start: Option<Instant>,
}
impl CheckpointCycle {
  fn new<T>(
    segments: T,
    flusher: CacheFlusher,
    log_id: LogId,
    start: Option<Instant>,
  ) -> Self
  where
    T: Iterator<Item = WALSegment>,
  {
    Self {
      segments: segments.collect(),
      flusher,
      log_id,
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
  fn truncate_all(mut self) -> Result {
    for segment in self.drain_all() {
      segment.truncate()?;
    }
    Ok(())
  }
  fn drain_all(&mut self) -> impl Iterator<Item = WALSegment> + '_ {
    self.segments.drain(..)
  }
  const fn get_log_id(&self) -> LogId {
    self.log_id
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
  incoming: Arc<SegQueue<WALSegment>>,
  ticker: Box<IntervalWorkThread<()>>,
  /**
   * Shared storage for the active checkpoint cycle.
   *
   * The interval worker is the only mutator while running, but shutdown must take
   * over the current checkpoint context after closing the worker and finish it
   * synchronously.
   */
  cycle: Arc<AtomicCell<Option<CheckpointCycle>>>,
  worker: Arc<CheckpointWorker>,
}
impl Checkpoint {
  fn with_worker(
    worker: Arc<CheckpointWorker>,
    event_bus: Arc<EventBus>,
    metrics: Arc<MetricsRegistry>,
    flush_factor: f64,
  ) -> Arc<Self> {
    let incoming = SegQueue::new().to_arc();
    let cycle = AtomicCell::new(None).to_arc();
    let ticker = ThreadBuilder::new()
      .name("checkpoint")
      .stack_size(2 << 20)
      .interval(
        CHECKPOINT_TICK,
        checkpoint_loop(
          incoming.clone(),
          worker.clone(),
          event_bus.clone(),
          cycle.clone(),
          metrics,
          flush_factor,
        ),
      )
      .to_box();

    let this = Arc::new(Self {
      incoming,
      ticker,
      cycle,
      worker,
    });
    event_bus.register(&this);
    this
  }
  pub fn new(
    wal: Arc<WriteAheadLog>,
    block_cache: Arc<BlockCache>,
    version_visibility: Arc<VersionVisibility>,
    io_pool: Arc<IOPool>,
    blob_storage: Arc<BlobStorage>,
    event_bus: Arc<EventBus>,
    metrics: Arc<MetricsRegistry>,
    flush_factor: f64,
  ) -> Arc<Self> {
    let worker =
      CheckpointWorker::new(wal, block_cache, version_visibility, io_pool, blob_storage)
        .to_arc();
    Self::with_worker(worker, event_bus, metrics, flush_factor)
  }

  pub fn initial_checkpoint(
    wal: Arc<WriteAheadLog>,
    block_cache: Arc<BlockCache>,
    version_visibility: Arc<VersionVisibility>,
    io_pool: Arc<IOPool>,
    blob_storage: Arc<BlobStorage>,
    event_bus: Arc<EventBus>,
    metrics: Arc<MetricsRegistry>,
    flush_factor: f64,
  ) -> Result<Arc<Self>> {
    let worker =
      CheckpointWorker::new(wal, block_cache, version_visibility, io_pool, blob_storage)
        .to_arc();
    worker.run_hard()?;
    Ok(Self::with_worker(worker, event_bus, metrics, flush_factor))
  }

  fn failover(&self) {
    self.ticker.close();
    let _ = self.cycle.take();
    while self.incoming.pop().is_some() {}
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
      self.failover();
      return Ok(());
    }
    self.ticker.close();

    if self.incoming.is_empty() {
      let Some(mut cycle) = self.cycle.take() else {
        return Ok(());
      };

      cycle.flush_hard()?;
      self.worker.finalize_checkpoint(cycle.get_log_id())?;
      cycle.truncate_all()?;
      info!("last checkpoint completed.");
      return Ok(());
    }

    self.worker.run_hard()?;
    while let Some(segment) = self.incoming.pop() {
      segment.truncate()?;
    }

    if let Some(cycle) = self.cycle.take() {
      cycle.truncate_all()?;
    }

    info!("last checkpoint completed.");
    Ok(())
  }
}

impl OwnedSubscription<WALSegmentRotated> for Checkpoint {
  fn handle(&self, event: WALSegmentRotated) {
    self.incoming.push(event.into_inner())
  }
}
impl SharedSubscription<WALFailed> for Checkpoint {
  fn handle(&self, _: Arc<WALFailed>) {
    error!("checkpoint stopped since wal failure detected.");
    self.failover();
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
  version_visibility: Arc<VersionVisibility>,
  io_pool: Arc<IOPool>,
  blob_storage: Arc<BlobStorage>,
}
impl CheckpointWorker {
  const fn new(
    wal: Arc<WriteAheadLog>,
    block_cache: Arc<BlockCache>,
    version_visibility: Arc<VersionVisibility>,
    io_pool: Arc<IOPool>,
    blob_storage: Arc<BlobStorage>,
  ) -> Self {
    Self {
      wal,
      block_cache,
      version_visibility,
      io_pool,
      blob_storage,
    }
  }

  fn finalize_checkpoint(&self, log_id: LogId) -> Result {
    let (current_version, active, aborted) = self.version_visibility.snapshot();
    let blobs = self.blob_storage.metadata_snapshot();
    let snapshot = CheckpointSnapshot::new(active, aborted, blobs);

    let current = PathBuf::from(uuid_simple()).with_extension(FILE_EXT);
    snapshot.write_at(&mut self.io_pool.open_append_io(current.clone())?)?;
    debug!("checkpoint snapshot persisted.");
    self.io_pool.sync_dir()?;

    self
      .wal
      .checkpoint_and_flush(log_id, current_version, current.clone())?;

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

  fn run_hard(&self) -> Result {
    let log_id = self.wal.current_log_id();
    info!("hard checkpoint trigger id {log_id}.");

    self.block_cache.create_flusher().flush_hard()?;
    self.finalize_checkpoint(log_id)?;
    info!("hard checkpoint complete id {log_id}");
    Ok(())
  }

  fn create_cycle(
    &self,
    incoming: &SegQueue<WALSegment>,
    metrics: &MetricsRegistry,
  ) -> CheckpointCycle {
    let log_id = self.wal.current_log_id();
    CheckpointCycle::new(
      repeat(()).map_while(|_| incoming.pop()),
      self.block_cache.create_flusher(),
      log_id,
      metrics.checkpoint_cycle.start(),
    )
  }

  fn run_tick<F: Fn(usize) -> usize>(
    &self,
    incoming: &SegQueue<WALSegment>,
    event_bus: &EventBus,
    cycle: &AtomicCell<Option<CheckpointCycle>>,
    metrics: &MetricsRegistry,
    calc_batch_size: &F,
  ) -> Result {
    // SAFETY: single threaded access to cycle.
    let cycle = unsafe { &mut *cycle.as_ptr() };
    let Some(current) = cycle else {
      let new = self.create_cycle(incoming, metrics);
      if new.is_empty() {
        debug!("no checkpoint required.");
        return Ok(());
      }

      info!(
        "new checkpoint cycle created for id {}, dirty blocks {}, segments {}",
        new.get_log_id(),
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
      let batch_size = calc_batch_size(current.segments_len() + incoming.len());
      trace!("checkpoint flush {} blocks", batch_size);
      return current.advance_flush(batch_size);
    }

    info!("checkpoint id {} trying to finish.", current.get_log_id());

    current.finish_flush()?;
    debug!("block cache all flushed.");

    if current.segments_len() == 0 {
      debug!("skip create checkpoint snapshot since nothing to rotate.");
      metrics.checkpoint_cycle.record(current.take_start());
      *cycle = None;
      return Ok(());
    }

    self.finalize_checkpoint(current.get_log_id())?;
    metrics.checkpoint_cycle.record(current.take_start());
    info!("checkpoint complete id {}", current.get_log_id());

    let events = current.drain_all().map(SegmentReuseable::new);
    event_bus.batch_publish(events);
    *cycle = None;
    Ok(())
  }
}

/**
 * Adaptive incremental checkpoint.
 * As the pressure to replace Wal segments increases,
 * more cache blocks are flushed.
 */
fn checkpoint_loop(
  incoming: Arc<SegQueue<WALSegment>>,
  worker: Arc<CheckpointWorker>,
  event_bus: Arc<EventBus>,
  cycle: Arc<AtomicCell<Option<CheckpointCycle>>>,
  metrics: Arc<MetricsRegistry>,
  flush_factor: f64,
) -> impl FnMut(Option<()>) {
  let mut calculated = [0f64; 20];
  calculated[0] = BATCH_SIZE;
  for i in 1..calculated.len() {
    calculated[i] = calculated[i - 1] * flush_factor;
  }

  let calc_batch_size = move |pressure: usize| -> usize {
    if pressure < calculated.len() {
      return calculated[pressure] as usize;
    }

    let last = calculated.len() - 1;
    (flush_factor.powi((pressure - last) as i32) * calculated[last]) as usize
  };

  move |_| {
    worker
      .run_tick(&incoming, &event_bus, &cycle, &metrics, &calc_batch_size)
      .unwrap()
  }
}
