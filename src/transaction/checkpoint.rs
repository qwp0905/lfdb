use std::{
  cell::UnsafeCell,
  collections::VecDeque,
  panic::RefUnwindSafe,
  sync::Arc,
  time::{Duration, Instant},
};

use crossbeam::queue::SegQueue;

use super::VersionVisibility;

use crate::{
  background::{BackgroundThread, EventBus, OwnedSubscription, WorkBuilder},
  binding_events,
  cache::{BlockCache, CacheFlusher},
  debug,
  disk::{IOPool, PAGE_SIZE},
  info,
  metrics::MetricsRegistry,
  trace,
  utils::{ToArc, ToBox},
  wal::{LogId, SegmentReuseable, WALSegment, WALSegmentRotated, WAL},
  Result,
};

const CHECKPOINT_TICK: Duration = Duration::from_millis(500);
const BATCH_SIZE: f64 = ((1 << 20) / PAGE_SIZE / 2) as f64; // convert from mib/sec

struct CheckpointCycle {
  segments: VecDeque<WALSegment>,
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
  fn segments_len(&self) -> usize {
    self.segments.len()
  }

  fn take_start(&mut self) -> Option<Instant> {
    self.start.take()
  }
}

struct CheckpointCell(UnsafeCell<Option<CheckpointCycle>>);
impl CheckpointCell {
  const fn as_mut(&self) -> &mut Option<CheckpointCycle> {
    unsafe { &mut *self.0.get() }
  }
  fn set(&self, cycle: CheckpointCycle) {
    unsafe { self.0.get().replace(Some(cycle)) };
  }
  fn clear(&self) {
    unsafe { self.0.get().replace(None) };
  }
}

unsafe impl Send for CheckpointCell {}
unsafe impl Sync for CheckpointCell {}
impl RefUnwindSafe for CheckpointCell {}

pub struct Checkpoint {
  incoming: Arc<SegQueue<WALSegment>>,
  ticker: Box<dyn BackgroundThread<(), Result>>,
  cycle: Arc<CheckpointCell>,
  wal: Arc<WAL>,
  block_cache: Arc<BlockCache>,
  version_visibility: Arc<VersionVisibility>,
  io_pool: Arc<IOPool>,
}
impl Checkpoint {
  pub fn new(
    wal: Arc<WAL>,
    block_cache: Arc<BlockCache>,
    version_visibility: Arc<VersionVisibility>,
    io_pool: Arc<IOPool>,
    event_bus: Arc<EventBus>,
    metrics: Arc<MetricsRegistry>,
    flush_factor: f64,
  ) -> Arc<Self> {
    let incoming = SegQueue::new().to_arc();
    let cycle = CheckpointCell(UnsafeCell::new(None)).to_arc();
    let ticker = WorkBuilder::new()
      .name("checkpoint")
      .single()
      .interval(
        CHECKPOINT_TICK,
        checkpoint_loop(
          incoming.clone(),
          wal.clone(),
          block_cache.clone(),
          version_visibility.clone(),
          io_pool.clone(),
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
      wal,
      block_cache,
      version_visibility,
      io_pool,
    });
    event_bus.register(&this);
    this
  }

  pub fn initial_checkpoint(
    wal: Arc<WAL>,
    block_cache: Arc<BlockCache>,
    version_visibility: Arc<VersionVisibility>,
    io_pool: Arc<IOPool>,
    event_bus: Arc<EventBus>,
    metrics: Arc<MetricsRegistry>,
    flush_factor: f64,
  ) -> Result<Arc<Self>> {
    Self::run_hard(&wal, &block_cache, &version_visibility, &io_pool)?;

    Ok(Self::new(
      wal,
      block_cache,
      version_visibility,
      io_pool,
      event_bus,
      metrics,
      flush_factor,
    ))
  }

  fn run_hard(
    wal: &WAL,
    block_cache: &BlockCache,
    version: &VersionVisibility,
    io_pool: &IOPool,
  ) -> Result {
    let log_id = wal.current_log_id();
    info!("hard checkpoint trigger id {log_id}.");

    block_cache.create_flusher().flush_hard()?;
    let (current_version, path) = version.persist_snapshot()?;
    io_pool.sync_dir()?;

    wal.checkpoint_and_flush(log_id, current_version, path.clone())?;
    info!("hard checkpoint complete id {log_id}");

    version.clear(&path)?;
    Ok(())
  }

  pub fn close(&self) -> Result {
    self.ticker.close();

    if self.incoming.is_empty() {
      let mut cycle = match self.cycle.as_mut().take() {
        Some(cycle) => cycle,
        None => return Ok(()),
      };

      cycle.flush_hard()?;
      finalize_checkpoint(
        &self.version_visibility,
        &self.io_pool,
        &self.wal,
        cycle.get_log_id(),
      )?;
      cycle.truncate_all()?;
      return Ok(());
    }

    Self::run_hard(
      &self.wal,
      &self.block_cache,
      &self.version_visibility,
      &self.io_pool,
    )?;

    while let Some(segment) = self.incoming.pop() {
      segment.truncate()?;
    }

    if let Some(cycle) = self.cycle.as_mut().take() {
      cycle.truncate_all()?;
    }

    Ok(())
  }
}

impl OwnedSubscription<WALSegmentRotated> for Checkpoint {
  fn handle(&self, event: WALSegmentRotated) {
    self.incoming.push(event.into_inner())
  }
}
binding_events!(Checkpoint {
  owned: [WALSegmentRotated]
});

/**
 * Adaptive incremental checkpoint.
 * As the pressure to replace Wal segments increases,
 * more cache blocks are flushed.
 */
fn checkpoint_loop(
  incoming: Arc<SegQueue<WALSegment>>,
  wal: Arc<WAL>,
  block_cache: Arc<BlockCache>,
  version: Arc<VersionVisibility>,
  io_pool: Arc<IOPool>,
  event_bus: Arc<EventBus>,
  cycle: Arc<CheckpointCell>,
  metrics: Arc<MetricsRegistry>,
  flush_factor: f64,
) -> impl FnMut(Option<()>) -> Result {
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
    let current = match cycle.as_mut() {
      Some(v) => v,
      None => {
        let log_id = wal.current_log_id();
        cycle.set(CheckpointCycle::new(
          (0..).map_while(|_| incoming.pop()),
          block_cache.create_flusher(),
          log_id,
          metrics.checkpoint_cycle.start(),
        ));
        return Ok(());
      }
    };

    if !current.flusher.is_done() {
      let batch_size = calc_batch_size(current.segments_len() + incoming.len());
      trace!("checkpoint flush {} blocks", batch_size);
      current.advance_flush(batch_size)?;
      return Ok(());
    }

    info!("checkpoint id {} trying to finish.", current.get_log_id());

    current.finish_flush()?;
    debug!("block cache all flushed.");

    finalize_checkpoint(&version, &io_pool, &wal, current.get_log_id())?;
    metrics.checkpoint_cycle.record(current.take_start());

    let events = current.drain_all().map(SegmentReuseable::new);
    event_bus.batch_publish(events);

    return Ok(cycle.clear());
  }
}

fn finalize_checkpoint(
  version: &VersionVisibility,
  io_pool: &IOPool,
  wal: &WAL,
  log_id: LogId,
) -> Result {
  let (current_version, path) = version.persist_snapshot()?;
  debug!("checkpoint snapshot persisted.");
  io_pool.sync_dir()?;

  wal.checkpoint_and_flush(log_id, current_version, path.clone())?;
  info!("checkpoint complete id {}", log_id);

  version.clear(&path)?;
  Ok(())
}
