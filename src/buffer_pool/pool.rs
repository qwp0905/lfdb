use std::{
  collections::BTreeMap, mem::MaybeUninit, ptr::drop_in_place, sync::Arc, time::Duration,
};

use crossbeam::epoch::{self, Guard};

use super::{Acquired, EvictionPin, Frame, LRUTable, Peeked, Slot};
use crate::{
  disk::{PagePool, Pointer, PAGE_SIZE},
  error::Result,
  metrics::MetricsRegistry,
  table::TableHandle,
  thread::{once, BackgroundThread, TaskHandle, WorkBuilder},
  utils::{AtomicBitmap, LogFilter, ToArc, ToBox},
};

const PRE_FLUSH_THRESHOLD: usize = 100;
const PRE_FLUSH_INTERVAL: Duration = Duration::from_millis(500);

pub struct BufferPoolConfig {
  pub shard_count: usize,
  pub capacity: usize,
}

pub struct BufferPool {
  table: LRUTable,
  frame: Arc<Vec<MaybeUninit<Frame>>>,
  pins: Arc<Vec<EvictionPin>>,
  dirty: Arc<AtomicBitmap>,
  logger: LogFilter,
  page_pool: Arc<PagePool<PAGE_SIZE>>,
  pre_flush: Box<dyn BackgroundThread<(), Result>>,
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
    let mut pins = Vec::with_capacity(frame_cap);
    pins.resize_with(frame_cap, EvictionPin::new);

    let frame = frame.to_arc();
    let pins = pins.to_arc();
    let dirty = AtomicBitmap::new(frame_cap).to_arc();

    let pre_flush = WorkBuilder::new()
      .name("buffer pool pre-flush")
      .single()
      .interval(
        PRE_FLUSH_INTERVAL,
        handle_flush(frame.clone(), pins.clone(), dirty.clone()),
      )
      .to_box();

    Ok(Self {
      frame,
      pins,
      table: LRUTable::new(config.shard_count, frame_cap),
      dirty,
      logger,
      page_pool,
      pre_flush,
      metrics,
    })
  }

  #[inline]
  fn page_slot(&self, id: usize) -> Slot<'_> {
    Slot::page(
      unsafe { self.frame[id].assume_init_ref() },
      &self.dirty,
      id,
      &self.pins[id],
      &self.page_pool,
    )
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
    let (state, guard) = match self
      .table
      .peek_or_temp(table_id, pointer, |id| &self.pins[id])
    {
      Peeked::Hit(id) => return Ok(self.page_slot(id)),
      Peeked::Temp(state) => {
        return Ok(Slot::temp(state, pointer, None, &self.page_pool))
      }
      Peeked::DiskRead(state, guard) => (state, guard),
    };

    let mut page = self.page_pool.acquire();
    handle.disk().read(pointer, &mut page)?;
    state.store(page);
    state.completion_evict();
    Ok(Slot::temp(
      state,
      pointer,
      Some((guard, handle)),
      &self.page_pool,
    ))
  }

  fn __read(&self, pointer: Pointer, handle: Arc<TableHandle>) -> Result<Slot<'_>> {
    let table_id = handle.metadata().get_id();
    let mut guard = match self.table.acquire(table_id, pointer, |id| &self.pins[id]) {
      Acquired::Temp(state) => {
        self.metrics.buffer_pool_cache_hit.inc();
        return Ok(Slot::temp(state, pointer, None, &self.page_pool));
      }
      Acquired::Hit(frame_id) => {
        self.metrics.buffer_pool_cache_hit.inc();
        return Ok(self.page_slot(frame_id));
      }
      Acquired::Evicted(guard) => guard,
    };

    let frame_id = guard.get_frame_id();
    let mut new = self.page_pool.acquire();
    handle.disk().read(pointer, &mut new)?;

    let new_frame = Frame::new(pointer, new, handle);
    let ptr = self.frame[frame_id].as_ptr() as *mut Frame;
    if !guard.is_evicted() {
      unsafe { ptr.write(new_frame) };
      guard.commit();
      return Ok(self.page_slot(frame_id));
    }

    let old = unsafe { ptr.replace(new_frame) };
    if self.dirty.contains(frame_id) {
      let guard = epoch::pin();
      old
        .flush_async(&guard)
        .map(|h| h.wait())
        .unwrap_or(Ok(()))?;
      self.dirty.remove(frame_id);
    }

    guard.commit();
    Ok(self.page_slot(frame_id))
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
    self
      .metrics
      .buffer_pool_flush
      .measure(|| self.pre_flush.execute(()).wait().flatten())?;
    self.logger.debug(|| "buffer pool synced.");
    Ok(())
  }

  pub fn close(&self) {
    self.pre_flush.close();
  }
}

impl Drop for BufferPool {
  fn drop(&mut self) {
    for (len, offset) in self.table.len_per_shard() {
      for i in offset..offset + len {
        unsafe { drop_in_place(self.frame[i].as_ptr() as *mut Frame) };
      }
    }
  }
}

fn handle_flush(
  frame: Arc<Vec<MaybeUninit<Frame>>>,
  pins: Arc<Vec<EvictionPin>>,
  dirty: Arc<AtomicBitmap>,
) -> impl Fn(Option<()>) -> Result {
  const MAX_BATCHING: usize = PRE_FLUSH_THRESHOLD;
  fn flush(waiting: &mut Vec<(Guard, TaskHandle<()>)>) -> Result {
    waiting.drain(..).map(|(_guard, w)| w.wait()).collect()
  }

  move |trigger| {
    let mut waits = Vec::with_capacity(PRE_FLUSH_THRESHOLD);
    let mut handles = BTreeMap::new();

    if trigger.is_none() && dirty.len() < PRE_FLUSH_THRESHOLD {
      return Ok(());
    }

    for id in dirty
      .iter()
      .take(trigger.map(|_| usize::MAX).unwrap_or(PRE_FLUSH_THRESHOLD))
    {
      let pin = &pins[id];
      if !pin.try_evict() {
        continue;
      }

      let frame = unsafe { frame[id].assume_init_ref() };
      {
        let _lock = frame.latch();
        if !dirty.remove(id) {
          pin.release();
          continue;
        };

        // Submit all writes asynchronously first so the DiskController can
        // buffer and sort them, then batch into a single pwritev call.
        // Writing synchronously one by one would bypass this optimization
        // and issue a separate syscall per page.
        let guard = epoch::pin();
        if let Some(r) = frame.flush_async(&guard) {
          waits.push((guard, r));
          handles
            .entry(frame.handle().metadata().get_id())
            .or_insert_with(|| frame.handle().clone());
        }
      }
      pin.release();

      if waits.len() < MAX_BATCHING {
        continue;
      }
      flush(&mut waits)?;
    }

    flush(&mut waits)?;

    handles
      .into_values()
      .flat_map(|handle| handle.try_pin())
      .map(|handle| once(move || handle.disk().fsync()))
      .collect::<Vec<_>>()
      .into_iter()
      .map(|th| th.wait().flatten())
      .collect::<Result>()?;

    Ok(())
  }
}
