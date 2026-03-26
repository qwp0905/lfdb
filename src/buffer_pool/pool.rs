use std::{
  path::PathBuf,
  sync::{Arc, RwLock},
};

use super::{Acquired, Frame, LRUTable, Peeked, Slot};
use crate::{
  disk::{DiskController, PagePool, PAGE_SIZE},
  error::Result,
  metrics::MetricsRegistry,
  utils::{Bitmap, LogFilter, ShortenedMutex, ShortenedRwLock, ToArc},
};

pub struct BufferPoolConfig {
  pub shard_count: usize,
  pub capacity: usize,
  pub path: PathBuf,
}

pub struct BufferPool {
  table: LRUTable,
  frame: Vec<RwLock<Frame>>,
  dirty: Bitmap,
  disk: DiskController<PAGE_SIZE>,
  logger: LogFilter,
  page_pool: Arc<PagePool<PAGE_SIZE>>,
  metrics: Arc<MetricsRegistry>,
}
impl BufferPool {
  pub fn open(
    config: BufferPoolConfig,
    logger: LogFilter,
    metrics: Arc<MetricsRegistry>,
  ) -> Result<Self> {
    let page_pool = PagePool::new(config.capacity).to_arc();
    let disk = DiskController::open(config.path, page_pool.clone(), metrics.clone())?;

    let frame_cap = (config.capacity * 9) / 10; // 90% of page pool capacity 10% buffer for disk io
    let mut frame = Vec::with_capacity(frame_cap);
    frame.resize_with(frame_cap, || RwLock::new(Frame::empty(page_pool.acquire())));

    Ok(Self {
      frame,
      disk,
      table: LRUTable::new(page_pool.clone(), config.shard_count, frame_cap),
      dirty: Bitmap::new(config.capacity),
      logger,
      page_pool,
      metrics,
    })
  }

  pub fn peek(&self, index: usize) -> Result<Slot<'_>> {
    let (state, shard) = match self.table.peek_or_temp(index) {
      Peeked::Hit(state) => {
        let id = state.get_frame_id();
        return Ok(Slot::page(&self.frame[id], &self.dirty, state));
      }
      Peeked::Temp(temp) => {
        let (state, shard) = temp.take();
        return Ok(Slot::temp(state, index, &self.disk, shard, false));
      }
      Peeked::DiskRead(temp) => temp.take(),
    };
    match self.disk.read(index, &mut state.for_write()) {
      Ok(p) => p,
      Err(err) => {
        shard.l().remove_temp(index);
        return Err(err);
      }
    };
    state.completion_evict(1);
    Ok(Slot::temp(state, index, &self.disk, shard, true))
  }

  pub fn read(&self, index: usize) -> Result<Slot<'_>> {
    let mut guard = match self.table.acquire(index) {
      Acquired::Temp(temp) => {
        let (state, shard) = temp.take();
        self.metrics.buffer_pool_cache_hit.inc();
        return Ok(Slot::temp(state, index, &self.disk, shard, false));
      }
      Acquired::Hit(state) => {
        let id = state.get_frame_id();
        self.metrics.buffer_pool_cache_hit.inc();
        return Ok(Slot::page(&self.frame[id], &self.dirty, state));
      }
      Acquired::Evicted(guard) => guard,
    };

    let id = guard.get_frame_id();
    let frame = &self.frame[id];
    let slot = Slot::page(frame, &self.dirty, guard.get_state());

    let mut new = self.page_pool.acquire();
    self.disk.read(index, &mut new)?;
    let old = frame.wl().replace(index, new);

    guard
      .get_evicted_index()
      .and_then(|i| self.dirty.remove(id).then(|| i))
      .map(|evicted| self.disk.write(evicted, &old))
      .unwrap_or(Ok(()))?;
    guard.commit();
    self.metrics.buffer_pool_cache_miss.inc();
    Ok(slot)
  }

  pub fn flush(&self) -> Result {
    self.logger.debug(|| "buffer pool flush triggered.");
    self.metrics.buffer_pool_flush.measure(|| {
      let mut waits = Vec::new();
      for id in self.dirty.iter() {
        let frame = self.frame[id].rl();
        self.dirty.remove(id);
        waits.push(self.disk.write_async(frame.get_index(), frame.page_ref()));
      }

      waits.into_iter().map(|w| w.wait()).collect::<Result>()?;
      self.logger.debug(|| "buffer pool flushed all pages.");

      self.disk.fsync()
    })?;

    self.logger.debug(|| "buffer pool synced.");
    Ok(())
  }

  pub fn close(&self) {
    self.disk.close();
  }

  pub fn disk_len(&self) -> Result<usize> {
    self.disk.len()
  }
}
