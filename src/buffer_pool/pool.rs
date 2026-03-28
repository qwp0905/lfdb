use std::{
  path::PathBuf,
  sync::{Arc, RwLock},
};

use super::{Acquired, Frame, LRUTable, Peeked, Slot};
use crate::{
  disk::{DiskController, PagePool, PAGE_SIZE},
  error::Result,
  metrics::MetricsRegistry,
  utils::{AtomicBitmap, LogFilter, ShortenedMutex, ShortenedRwLock, ToArc},
};

pub struct BufferPoolConfig {
  pub shard_count: usize,
  pub capacity: usize,
  pub path: PathBuf,
}

pub struct BufferPool {
  table: LRUTable,
  frame: Vec<RwLock<Frame>>,
  dirty: AtomicBitmap,
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

    // 90% of page pool capacity reserved for frames; the remaining 10% is kept
    // free for DiskController writes, which acquire a pooled page per write to
    // copy data before releasing the frame lock.
    let frame_cap = (config.capacity * 9) / 10;
    let mut frame = Vec::with_capacity(frame_cap);
    frame.resize_with(frame_cap, || RwLock::new(Frame::empty(page_pool.acquire())));

    Ok(Self {
      frame,
      disk,
      table: LRUTable::new(page_pool.clone(), config.shard_count, frame_cap),
      dirty: AtomicBitmap::new(frame_cap),
      logger,
      page_pool,
      metrics,
    })
  }

  /**
   * Reading buffer pool without promotion.
   * Used for reads that should not affect LRU, like gc.
   *
   * DiskRead: another thread has already registered a temp slot for this
   * page but hasn't finished loading it yet. We must read from disk here
   * rather than waiting, but we still take the temp slot to prevent a
   * concurrent read() from promoting this page into the LRU while we
   * are reading — which would create two live copies of the same page.
   */
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
        // Remove the temp entry so other threads waiting on this page
        // don't block forever on a completion signal that will never arrive.
        shard.l().remove_temp(index);
        return Err(err);
      }
    };
    state.completion_evict(1);
    Ok(Slot::temp(state, index, &self.disk, shard, true))
  }

  fn __read(&self, index: usize) -> Result<Slot<'_>> {
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

    let mut new = self.page_pool.acquire();
    self.disk.read(index, &mut new)?;
    let old = frame.wl().replace(index, new);

    let slot = Slot::page(frame, &self.dirty, guard.get_state());
    if let Some(evicted) = guard.get_evicted_index() {
      if self.dirty.contains(id) {
        self.disk.write(evicted, &old)?;
        self.dirty.remove(id);
      }
    }

    guard.commit();
    Ok(slot)
  }

  #[inline]
  pub fn read(&self, index: usize) -> Result<Slot<'_>> {
    self.metrics.buffer_pool_read.measure(|| self.__read(index))
  }

  pub fn flush(&self) -> Result {
    self.logger.debug(|| "buffer pool flush triggered.");
    self.metrics.buffer_pool_flush.measure(|| {
      let mut waits = Vec::new();
      for id in self.dirty.iter() {
        let frame = self.frame[id].rl();
        self.dirty.remove(id);

        // Submit all writes asynchronously first so the DiskController can
        // buffer and sort them, then batch into a single pwritev call.
        // Writing synchronously one by one would bypass this optimization
        // and issue a separate syscall per page.
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
