use std::{
  collections::VecDeque,
  iter::repeat_with,
  sync::Arc,
  time::{Duration, Instant},
};

use crossbeam::{atomic::AtomicCell, queue::SegQueue};

use super::VersionVisibility;

use crate::{
  background::{
    BackgroundThread, EventBus, OwnedSubscription, SharedSubscription, WorkBuilder,
  },
  binding_events,
  cache::{BlockCache, CacheFlusher},
  debug,
  disk::{IOPool, PAGE_SIZE},
  error, info,
  metrics::MetricsRegistry,
  trace,
  utils::{ToArc, ToBox, UnsafeBorrowMut},
  wal::{LogId, SegmentReuseable, WALFailed, WALSegment, WALSegmentRotated, WAL},
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
  fn dirty_len(&self) -> usize {
    self.flusher.len()
  }
  fn take_start(&mut self) -> Option<Instant> {
    self.start.take()
  }
}

pub struct Checkpoint {
  incoming: Arc<SegQueue<WALSegment>>,
  ticker: Box<dyn BackgroundThread<(), Result>>,
  cycle: Arc<AtomicCell<Option<CheckpointCycle>>>,
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
    let cycle = AtomicCell::new(None).to_arc();
    let ticker = WorkBuilder::new()
      .name("checkpoint")
      .stack_size(2 << 20)
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

  fn failover(&self) {
    self.ticker.close();
    let _ = self.cycle.take();
    while let Some(_) = self.incoming.pop() {}
  }

  pub fn close(&self) -> Result {
    if !self.wal.is_available() {
      self.failover();
      return Ok(());
    }
    self.ticker.close();

    if self.incoming.is_empty() {
      let mut cycle = match self.cycle.take() {
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
  cycle: Arc<AtomicCell<Option<CheckpointCycle>>>,
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
    let cycle = cycle.as_ptr().borrow_mut_unsafe();
    let current = match cycle {
      Some(v) => v,
      None => {
        let log_id = wal.current_log_id();
        let new = cycle.insert(CheckpointCycle::new(
          repeat_with(|| incoming.pop()).map_while(|v| v),
          block_cache.create_flusher(),
          log_id,
          metrics.checkpoint_cycle.start(),
        ));

        info!(
          "new checkpoint cycle created for id {log_id}, dirty blocks {}, segments {}",
          new.dirty_len(),
          new.segments_len(),
        );
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

    return Ok(*cycle = None);
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
