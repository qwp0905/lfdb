use std::{collections::VecDeque, mem::take, sync::Arc, time::Duration};

use crossbeam::queue::SegQueue;

use super::VersionVisibility;

use crate::{
  background::{BackgroundThread, EventBus, OwnedSubscription, WorkBuilder},
  binding_events,
  cache::{BlockCache, CacheFlusher},
  debug,
  disk::{IOPool, PAGE_SIZE},
  info, trace,
  utils::{ToArc, ToBox},
  wal::{LogId, SegmentReuseable, WALSegment, WALSegmentRotated, WAL},
  Result,
};

const CHECKPOINT_TICK: Duration = Duration::from_millis(500);
const BATCH_SIZE: f64 = ((1 << 20) / PAGE_SIZE / 2) as f64; // convert from mib/sec

pub struct Checkpoint {
  incoming: Arc<SegQueue<WALSegment>>,
  ticker: Box<dyn BackgroundThread<(), Result>>,
}
impl Checkpoint {
  pub fn new(
    wal: Arc<WAL>,
    block_cache: Arc<BlockCache>,
    version_visibility: Arc<VersionVisibility>,
    io_pool: Arc<IOPool>,
    event_bus: Arc<EventBus>,
    flush_factor: f64,
  ) -> Arc<Self> {
    let incoming = SegQueue::new().to_arc();
    let ticker = WorkBuilder::new()
      .name("checkpoint")
      .single()
      .interval(
        CHECKPOINT_TICK,
        checkpoint_loop(
          incoming.clone(),
          wal,
          block_cache,
          version_visibility,
          io_pool,
          event_bus.clone(),
          flush_factor,
        ),
      )
      .to_box();

    let this = Arc::new(Self { incoming, ticker });
    event_bus.register(&this);
    this
  }

  pub fn run_hard(
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

  pub fn close(&self) {
    self.ticker.close();
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

struct CheckpointCycle {
  reuse_target: VecDeque<WALSegment>,
  flusher: CacheFlusher,
  log_id: LogId,
}

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
  flush_factor: f64,
) -> impl FnMut(Option<()>) -> Result {
  let mut income = VecDeque::new();
  let mut state: Option<CheckpointCycle> = None;

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
    while let Some(segment) = incoming.pop() {
      income.push_back(segment);
    }

    let current = match state.as_mut() {
      Some(v) => v,
      None => {
        let log_id = wal.current_log_id();
        state = Some(CheckpointCycle {
          reuse_target: take(&mut income),
          flusher: block_cache.create_flusher(),
          log_id,
        });
        return Ok(());
      }
    };

    if !current.flusher.is_done() {
      let batch_size = calc_batch_size(current.reuse_target.len() + income.len());
      trace!("checkpoint flush {} blocks", batch_size);
      current.flusher.advance(batch_size)?;
      return Ok(());
    }

    info!("checkpoint id {} trying to finish.", current.log_id);

    current.flusher.finish()?;
    debug!("block cache all flushed.");

    let (current_version, path) = version.persist_snapshot()?;
    debug!("checkpoint snapshot persisted.");
    io_pool.sync_dir()?;

    wal.checkpoint_and_flush(current.log_id, current_version, path.clone())?;
    info!("checkpoint complete id {}", current.log_id);

    version.clear(&path)?;

    let events = current.reuse_target.drain(..).map(SegmentReuseable::new);
    event_bus.batch_publish(events);

    return Ok(state = None);
  }
}
