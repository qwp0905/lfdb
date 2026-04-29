use std::{mem::MaybeUninit, ptr::drop_in_place, sync::Arc, time::Duration};

use crossbeam::epoch::{self, Guard};

use super::{Acquired, CacheSlot, CachedBlock, DirtyTables, LRUTable, Peeked};
use crate::{
  debug,
  disk::{PagePool, Pointer, PAGE_SIZE},
  error::Result,
  metrics::MetricsRegistry,
  table::TableHandle,
  thread::{once, BackgroundThread, TaskHandle, WorkBuilder},
  utils::{AtomicBitmap, ExclusivePin, SharedToken, ToArc, ToBox},
};

const PRE_FLUSH_THRESHOLD: usize = 100;
const PRE_FLUSH_INTERVAL: Duration = Duration::from_millis(500);

pub struct BlockCacheConfig {
  pub shard_count: usize,
  pub capacity: usize,
}

pub struct BlockCache {
  table: LRUTable,
  cached_blocks: Arc<Vec<MaybeUninit<CachedBlock>>>,
  /**
   * pin to protect each block in cached blocks from eviction
   */
  pins: Arc<Vec<ExclusivePin>>,
  dirty_frames: Arc<AtomicBitmap>,
  page_pool: Arc<PagePool<PAGE_SIZE>>,
  pre_flush: Box<dyn BackgroundThread<(), Result>>,
  metrics: Arc<MetricsRegistry>,
  dirty_tables: Arc<DirtyTables>,
}
impl BlockCache {
  pub fn open(config: BlockCacheConfig, metrics: Arc<MetricsRegistry>) -> Result<Self> {
    let page_pool = PagePool::new(config.capacity).to_arc();

    // 90% of page pool capacity reserved for blocks; the remaining 10% is kept
    // free for copy on write.
    let block_cap = (config.capacity * 9) / 10;
    let mut blocks = Vec::with_capacity(block_cap);
    blocks.resize_with(block_cap, MaybeUninit::uninit);
    let mut pins = Vec::with_capacity(block_cap);
    pins.resize_with(block_cap, ExclusivePin::new);

    let cached_blocks = blocks.to_arc();
    let pins = pins.to_arc();
    let dirty = AtomicBitmap::new(block_cap).to_arc();

    let dirty_tables = DirtyTables::new().to_arc();

    let pre_flush = WorkBuilder::new()
      .name("block cache pre-flush")
      .single()
      .interval(
        PRE_FLUSH_INTERVAL,
        handle_flush(
          cached_blocks.clone(),
          pins.clone(),
          dirty.clone(),
          dirty_tables.clone(),
        ),
      )
      .to_box();

    Ok(Self {
      cached_blocks,
      pins,
      table: LRUTable::new(config.shard_count, block_cap),
      dirty_frames: dirty,
      page_pool,
      pre_flush,
      metrics,
      dirty_tables,
    })
  }

  #[inline]
  fn page_slot<'a>(&'a self, id: usize, token: SharedToken<'a>) -> CacheSlot<'a> {
    CacheSlot::page(
      unsafe { self.cached_blocks[id].assume_init_ref() },
      &self.dirty_frames,
      id,
      token,
      &self.page_pool,
    )
  }

  /**
   * Reading block cache without promotion.
   * Used for reads that should not affect LRU, like gc.
   *
   * DiskRead: another thread has already registered a temp slot for this
   * page but hasn't finished loading it yet. We must read from disk here
   * rather than waiting, but we still take the temp slot to prevent a
   * concurrent read() from promoting this page into the LRU while we
   * are reading — which would create two live copies of the same page.
   */
  pub fn peek(
    &self,
    pointer: Pointer,
    handle: Arc<TableHandle>,
  ) -> Result<CacheSlot<'_>> {
    let table_id = handle.metadata().get_id();
    let (state_ref, guard) = match self
      .table
      .peek_or_temp(table_id, pointer, |id| &self.pins[id])
    {
      Peeked::Hit(id, token) => return Ok(self.page_slot(id, token)),
      Peeked::Temp(state) => {
        return Ok(CacheSlot::temp(state, pointer, None, &self.page_pool))
      }
      Peeked::DiskRead(state, guard) => (state, guard),
    };

    let mut page = self.page_pool.acquire();
    handle.disk().read(pointer, &mut page)?;
    state_ref.store(page);
    Ok(CacheSlot::temp(
      state_ref.downgrade(),
      pointer,
      Some((guard, handle, &self.dirty_tables)),
      &self.page_pool,
    ))
  }

  fn __read(&self, pointer: Pointer, handle: Arc<TableHandle>) -> Result<CacheSlot<'_>> {
    let table_id = handle.metadata().get_id();
    let guard = match self.table.acquire(table_id, pointer, |id| &self.pins[id]) {
      Acquired::Temp(state) => {
        self.metrics.block_cache_hit.inc();
        return Ok(CacheSlot::temp(state, pointer, None, &self.page_pool));
      }
      Acquired::Hit(block_id, token) => {
        self.metrics.block_cache_hit.inc();
        return Ok(self.page_slot(block_id, token));
      }
      Acquired::Evicted(guard) => guard,
    };

    let block_id = guard.get_block_id();
    let mut new = self.page_pool.acquire();
    handle.disk().read(pointer, &mut new)?;

    let new_block = CachedBlock::new(pointer, new, handle);
    let ptr = self.cached_blocks[block_id].as_ptr() as *mut CachedBlock;
    if !guard.is_evicted() {
      unsafe { ptr.write(new_block) };
      return Ok(self.page_slot(block_id, guard.commit()));
    }

    let old = unsafe { ptr.replace(new_block) };
    if self.dirty_frames.contains(block_id) {
      old.flush_async(&epoch::pin()).wait()?;
      self.dirty_frames.remove(block_id);
      self.dirty_tables.mark(old.handle());
    }

    Ok(self.page_slot(block_id, guard.commit()))
  }

  #[inline]
  pub fn read(
    &self,
    pointer: Pointer,
    handle: Arc<TableHandle>,
  ) -> Result<CacheSlot<'_>> {
    self
      .metrics
      .block_cache_read
      .measure(|| self.__read(pointer, handle))
  }

  pub fn flush(&self) -> Result {
    debug!("block cache flush triggered.");
    self
      .metrics
      .block_cache_flush
      .measure(|| self.pre_flush.execute(()).wait().flatten())?;
    debug!("block cache synced.");
    Ok(())
  }

  pub fn close(&self) {
    self.pre_flush.close();
  }
}

impl Drop for BlockCache {
  fn drop(&mut self) {
    for (len, offset) in self.table.len_per_shard() {
      for i in offset..offset + len {
        unsafe { drop_in_place(self.cached_blocks[i].as_ptr() as *mut CachedBlock) };
      }
    }
  }
}

fn __flush(waiting: &mut Vec<(TaskHandle<()>, Guard)>) -> Result {
  waiting.drain(..).map(|(w, _guard)| w.wait()).collect()
}
const MAX_BATCHING: usize = PRE_FLUSH_THRESHOLD;

const fn handle_flush(
  blocks: Arc<Vec<MaybeUninit<CachedBlock>>>,
  pins: Arc<Vec<ExclusivePin>>,
  dirty_frames: Arc<AtomicBitmap>,
  dirty_tables: Arc<DirtyTables>,
) -> impl Fn(Option<()>) -> Result {
  move |trigger| {
    let mut waits = Vec::with_capacity(MAX_BATCHING);

    if trigger.is_none() && dirty_frames.len() < PRE_FLUSH_THRESHOLD {
      return Ok(());
    }

    for id in dirty_frames
      .iter()
      .take(trigger.map(|_| usize::MAX).unwrap_or(PRE_FLUSH_THRESHOLD))
    {
      {
        let _token = match pins[id].try_shared() {
          Some(t) => t,
          None => continue,
        };

        let block = unsafe { blocks[id].assume_init_ref() };
        let _lock = block.latch();
        if !dirty_frames.remove(id) {
          continue;
        };

        // Submit all writes asynchronously first so the DiskController can
        // buffer and sort them, then batch into a single pwritev call.
        // Writing synchronously one by one would bypass this optimization
        // and issue a separate syscall per page.
        let guard = epoch::pin();
        waits.push((block.flush_async(&guard), guard));
        dirty_tables.mark(block.handle());
      }

      if waits.len() < MAX_BATCHING {
        continue;
      }
      __flush(&mut waits)?;
    }

    __flush(&mut waits)?;

    let mut threads = Vec::new();
    for table in dirty_tables.drain() {
      threads.push(once(move || table.disk().fsync()));
    }

    threads
      .into_iter()
      .map(|th| th.wait().flatten())
      .collect::<Result>()?;

    Ok(())
  }
}
