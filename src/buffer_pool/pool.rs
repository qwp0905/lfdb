use std::{
  collections::BTreeMap,
  mem::MaybeUninit,
  sync::{Arc, RwLock},
};

use super::{Acquired, Frame, LRUTable, Peeked, Slot};
use crate::{
  disk::{PagePool, Pointer, PAGE_SIZE},
  error::Result,
  metrics::MetricsRegistry,
  table::TableHandle,
  thread::once,
  utils::{
    AtomicBitmap, LogFilter, ShortenedMutex, ShortenedRwLock, ToArc, UnsafeBorrow,
  },
};

pub struct BufferPoolConfig {
  pub shard_count: usize,
  pub capacity: usize,
}

pub struct BufferPool {
  table: LRUTable,
  frame: Vec<MaybeUninit<RwLock<Frame>>>,
  dirty: AtomicBitmap,
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

    // 90% of page pool capacity reserved for frames; the remaining 10% is kept
    // free for DiskController writes, which acquire a pooled page per write to
    // copy data before releasing the frame lock.
    let frame_cap = (config.capacity * 9) / 10;
    let mut frame = Vec::with_capacity(frame_cap);
    frame.resize_with(frame_cap, MaybeUninit::uninit);

    Ok(Self {
      frame,
      // disk,
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
  pub fn peek(&self, pointer: Pointer, handle: Arc<TableHandle>) -> Result<Slot<'_>> {
    let table_id = handle.metadata().get_id();
    let (state, shard) = match self.table.peek_or_temp(table_id, pointer) {
      Peeked::Hit(state) => {
        let id = state.get_frame_id();
        return Ok(Slot::page(
          unsafe { self.frame[id].assume_init_ref() },
          &self.dirty,
          state,
        ));
      }
      Peeked::Temp(temp) => {
        let (state, shard) = temp.take();
        return Ok(Slot::temp(state, pointer, None, shard));
      }
      Peeked::DiskRead(temp) => temp.take(),
    };
    match handle.disk().read(pointer, &mut state.for_write()) {
      Ok(p) => p,
      Err(err) => {
        // Remove the temp entry so other threads waiting on this page
        // don't block forever on a completion signal that will never arrive.
        shard.l().remove_temp(table_id, pointer);
        return Err(err);
      }
    };
    state.completion_evict(1);
    Ok(Slot::temp(state, pointer, Some(handle), shard))
  }

  fn __read(&self, pointer: Pointer, handle: Arc<TableHandle>) -> Result<Slot<'_>> {
    let table_id = handle.metadata().get_id();
    let mut guard = match self.table.acquire(table_id, pointer) {
      Acquired::Temp(temp) => {
        let (state, shard) = temp.take();
        self.metrics.buffer_pool_cache_hit.inc();
        return Ok(Slot::temp(state, pointer, None, shard));
      }
      Acquired::Hit(state) => {
        let id = state.get_frame_id();
        self.metrics.buffer_pool_cache_hit.inc();
        return Ok(Slot::page(
          unsafe { self.frame[id].assume_init_ref() },
          &self.dirty,
          state,
        ));
      }
      Acquired::Evicted(guard) => guard,
    };

    let id = guard.get_frame_id();
    let mut new = self.page_pool.acquire();
    handle.disk().read(pointer, &mut new)?;

    let evicted = match guard.get_evicted_pointer() {
      Some(evicted) => evicted,
      None => {
        let ptr = self.frame[id].as_ptr() as *mut RwLock<_>;
        unsafe { ptr.write(RwLock::new(Frame::new(pointer, new, handle))) };
        guard.commit();
        return Ok(Slot::page(
          ptr.borrow_unsafe(),
          &self.dirty,
          guard.get_state(),
        ));
      }
    };

    let frame = unsafe { self.frame[id].assume_init_ref() };
    let (old_p, old_h) = frame.wl().replace(pointer, new, handle);

    if self.dirty.contains(id) {
      old_h.disk().write(evicted, &old_p)?;
      self.dirty.remove(id);
    }

    guard.commit();
    Ok(Slot::page(frame, &self.dirty, guard.get_state()))
  }

  #[inline]
  pub fn read(&self, pointer: Pointer, handle: Arc<TableHandle>) -> Result<Slot<'_>> {
    self
      .metrics
      .buffer_pool_read
      .measure(|| self.__read(pointer, handle))
  }

  pub fn flush(&self) -> Result {
    self.logger.debug(|| "buffer pool flush triggered.");
    self.metrics.buffer_pool_flush.measure(|| {
      let mut waits = Vec::new();
      let mut handles = BTreeMap::new();
      for id in self.dirty.iter() {
        let frame = unsafe { self.frame[id].assume_init_ref() }.rl();
        self.dirty.remove(id);

        // Submit all writes asynchronously first so the DiskController can
        // buffer and sort them, then batch into a single pwritev call.
        // Writing synchronously one by one would bypass this optimization
        // and issue a separate syscall per page.
        waits.push(frame.flush_async());

        let handle = frame.handle();
        handles
          .entry(handle.metadata().get_id())
          .or_insert_with(|| handle);
      }

      waits.into_iter().map(|w| w.wait()).collect::<Result>()?;
      self.logger.debug(|| "buffer pool flushed all pages.");

      handles
        .into_values()
        .map(|handle| once(move || handle.disk().fsync()))
        .collect::<Vec<_>>()
        .into_iter()
        .map(|th| th.wait().flatten())
        .collect::<Result>()?;

      Ok(())
    })?;

    self.logger.debug(|| "buffer pool synced.");
    Ok(())
  }

  pub fn page_pool(&self) -> Arc<PagePool<PAGE_SIZE>> {
    self.page_pool.clone()
  }
}

impl Drop for BufferPool {
  fn drop(&mut self) {
    for (len, offset) in self.table.len_per_shard() {
      for i in offset..offset + len {
        unsafe { self.frame[i].assume_init_drop() };
      }
    }
  }
}
