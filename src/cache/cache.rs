use std::{
  cell::UnsafeCell, mem::MaybeUninit, panic::RefUnwindSafe, sync::Arc, time::Duration,
};

use super::{
  Acquired, BatchHandle, BlockId, CachedBlock, CachedSlot, DirtyTables, EvictionGuard,
  MappingTable,
};
use crate::{
  debug,
  disk::{PagePool, Pointer, PAGE_SIZE},
  error::Result,
  metrics::MetricsRegistry,
  table::TableHandleRef,
  thread::{BackgroundThread, TaskHandle, WorkBuilder},
  utils::{AtomicBitmap, ExclusivePin, SharedToken, ToArc, ToBox},
};

pub struct BlockCacheConfig {
  pub shard_count: usize,
  pub capacity: usize,
}

struct BlockCell(UnsafeCell<MaybeUninit<CachedBlock>>);
impl BlockCell {
  const fn uninit() -> Self {
    Self(UnsafeCell::new(MaybeUninit::uninit()))
  }
  const fn get(&self) -> &CachedBlock {
    unsafe { (*self.0.get()).assume_init_ref() }
  }
  const fn write(&self, block: CachedBlock) {
    unsafe { (*self.0.get()).write(block) };
  }
  const fn replace(&self, block: CachedBlock) -> CachedBlock {
    unsafe { (*self.0.get()).as_mut_ptr().replace(block) }
  }
  fn drop_in_place(&self) {
    unsafe { (*self.0.get()).assume_init_drop() };
  }
}
unsafe impl Send for BlockCell {}
unsafe impl Sync for BlockCell {}
impl RefUnwindSafe for BlockCell {}

pub struct BlockCache {
  table: MappingTable,
  cached_blocks: Arc<[BlockCell]>,
  /**
   * pin to protect each block in cached blocks from eviction
   */
  pins: Arc<[ExclusivePin]>,
  /**
   * each dirty bits are protected by each block's latch
   */
  batch_handles: Box<[BatchHandle]>,
  dirty_blocks: Arc<AtomicBitmap>,
  page_pool: PagePool<PAGE_SIZE>,
  pre_flush: Box<dyn BackgroundThread<(), Result>>,
  flush_executor: Arc<dyn BackgroundThread<FlushTask, Result>>,
  metrics: Arc<MetricsRegistry>,
  dirty_tables: Arc<DirtyTables>,
}
impl BlockCache {
  pub fn open(config: BlockCacheConfig, metrics: Arc<MetricsRegistry>) -> Result<Self> {
    let page_pool = PagePool::new(config.capacity);

    // 90% of page pool capacity reserved for blocks; the remaining 10% is kept
    // free for copy on write.
    let block_cap = (config.capacity * 9) / 10;
    let mut blocks = Vec::with_capacity(block_cap);
    blocks.resize_with(block_cap, BlockCell::uninit);

    let mut pins = Vec::with_capacity(block_cap);
    pins.resize_with(block_cap, ExclusivePin::new);

    let mut batch_handles = Vec::with_capacity(block_cap);
    batch_handles.resize_with(block_cap, BatchHandle::new);

    let cached_blocks = Arc::<[_]>::from(blocks.into_boxed_slice());
    let pins = Arc::<[_]>::from(pins.into_boxed_slice());
    let batch_handles = batch_handles.into_boxed_slice();
    let dirty_blocks = AtomicBitmap::new(block_cap).to_arc();

    let dirty_tables = DirtyTables::new().to_arc();

    let flush_executor = WorkBuilder::new()
      .name("flush executor")
      .multi(MAX_BATCHING)
      .shared(handle_execute(
        cached_blocks.clone(),
        pins.clone(),
        dirty_blocks.clone(),
        dirty_tables.clone(),
      ))
      .to_arc();

    let pre_flush = WorkBuilder::new()
      .name("block cache pre-flush")
      .single()
      .interval(
        PRE_FLUSH_INTERVAL,
        handle_flush(
          flush_executor.clone(),
          dirty_blocks.clone(),
          dirty_tables.clone(),
        ),
      )
      .to_box();

    Ok(Self {
      cached_blocks,
      pins,
      batch_handles,
      table: MappingTable::new(config.shard_count, block_cap),
      dirty_blocks,
      page_pool,
      pre_flush,
      flush_executor,
      metrics,
      dirty_tables,
    })
  }

  #[inline]
  fn cache_slot<'a>(&'a self, id: usize, token: SharedToken<'a>) -> CachedSlot<'a> {
    CachedSlot::new(
      self.cached_blocks[id].get(),
      &self.dirty_blocks,
      &self.batch_handles[id],
      id,
      token,
      &self.page_pool,
    )
  }

  fn handle_eviction<'a>(
    &'a self,
    guard: EvictionGuard<'a>,
    new: CachedBlock,
  ) -> Result<CachedSlot<'a>> {
    let block_id = guard.get_block_id();
    if !guard.is_evicted() {
      self.cached_blocks[block_id].write(new);
      return Ok(self.cache_slot(block_id, guard.commit()));
    }

    let old = self.cached_blocks[block_id].replace(new);
    if self.dirty_blocks.contains(block_id) {
      // hard flush allowed since exclusive token acquired.
      if let Err(err) = old.flush_hard() {
        self.cached_blocks[block_id].replace(old);
        return Err(err);
      }
      self.dirty_blocks.remove(block_id);
      self.dirty_tables.mark(old.handle());
    }

    Ok(self.cache_slot(block_id, guard.commit()))
  }

  /**
   * Alloc new block without read from disk for allocate new block at disk.
   */
  pub fn alloc(
    &self,
    pointer: Pointer,
    handle: TableHandleRef,
  ) -> Result<CachedSlot<'_>> {
    let table_id = handle.metadata().get_id();
    let guard = self.table.alloc(table_id, pointer, |id| &self.pins[id]);

    let new_block = CachedBlock::new(pointer, self.page_pool.acquire(), handle);
    self.handle_eviction(guard, new_block)
  }

  /**
   * Call disk read unchecked method.
   * It is only allowed in bootstrap.
   */
  pub fn read_unchecked(
    &self,
    pointer: Pointer,
    handle: TableHandleRef,
  ) -> Result<CachedSlot<'_>> {
    let table_id = handle.metadata().get_id();
    let guard = match self.table.acquire(table_id, pointer, |id| &self.pins[id]) {
      Acquired::Hit(block_id, token) => return Ok(self.cache_slot(block_id, token)),
      Acquired::Evicted(guard) => guard,
    };

    let mut new = self.page_pool.acquire();
    handle.disk().read_unchecked(pointer, &mut new)?;
    let new_block = CachedBlock::new(pointer, new, handle);
    self.handle_eviction(guard, new_block)
  }

  fn __read(&self, pointer: Pointer, handle: TableHandleRef) -> Result<CachedSlot<'_>> {
    let table_id = handle.metadata().get_id();
    let guard = match self.table.acquire(table_id, pointer, |id| &self.pins[id]) {
      Acquired::Hit(block_id, token) => {
        self.metrics.block_cache_hit.inc();
        return Ok(self.cache_slot(block_id, token));
      }
      Acquired::Evicted(guard) => guard,
    };

    let mut new = self.page_pool.acquire();
    handle.disk().read(pointer, &mut new)?;
    let new_block = CachedBlock::new(pointer, new, handle);
    self.handle_eviction(guard, new_block)
  }

  #[inline]
  pub fn read(&self, pointer: Pointer, handle: TableHandleRef) -> Result<CachedSlot<'_>> {
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
    self.flush_executor.close();
  }
}

impl Drop for BlockCache {
  fn drop(&mut self) {
    for (len, offset) in self.table.len_per_shard() {
      for i in offset..offset + len {
        self.cached_blocks[i].drop_in_place();
      }
    }
  }
}

fn __flush(waiting: &mut Vec<TaskHandle<()>>) -> Result {
  waiting.drain(..).map(|w| w.wait()).collect()
}

const PRE_FLUSH_INTERVAL: Duration = Duration::from_millis(500);
const PRE_FLUSH_THRESHOLD: usize = 100;
const MAX_BATCHING: usize = 8;

enum FlushTask {
  Write(BlockId),
  Fsync(TableHandleRef),
}

const fn handle_execute(
  blocks: Arc<[BlockCell]>,
  pins: Arc<[ExclusivePin]>,
  dirty_blocks: Arc<AtomicBitmap>,
  dirty_tables: Arc<DirtyTables>,
) -> impl Fn(FlushTask) -> Result {
  move |task| match task {
    FlushTask::Write(id) => {
      let _token = match pins[id].try_shared() {
        Some(t) => t,
        None => return Ok(()),
      };

      let block = blocks[id].get();
      let flusher = block.flusher();
      if !dirty_blocks.remove(id) {
        return Ok(());
      }

      let result = flusher.submit();
      if let (epoch, Err(err)) = result.finalize() {
        let latch = block.latch();
        if latch.epoch() == epoch {
          dirty_blocks.insert(id);
        }
        return Err(err);
      }

      dirty_tables.mark(block.handle());
      Ok(())
    }
    FlushTask::Fsync(table) => table.disk().fsync(),
  }
}

const fn handle_flush(
  executor: Arc<dyn BackgroundThread<FlushTask, Result>>,
  dirty_blocks: Arc<AtomicBitmap>,
  dirty_tables: Arc<DirtyTables>,
) -> impl Fn(Option<()>) -> Result {
  move |trigger| {
    let mut waits = Vec::new();
    if trigger.is_none() {
      // periodical pre flush. it does not trigger fsync of a table.
      for id in dirty_blocks.iter().take(PRE_FLUSH_THRESHOLD) {
        waits.push(executor.execute(FlushTask::Write(id)));
      }

      waits
        .drain(..)
        .map(|done| done.wait().flatten())
        .collect::<Result>()?;
      return Ok(());
    }

    for id in dirty_blocks.iter() {
      waits.push(executor.execute(FlushTask::Write(id)));
    }

    waits
      .drain(..)
      .map(|done| done.wait().flatten())
      .collect::<Result>()?;

    for table in dirty_tables.drain() {
      waits.push(executor.execute(FlushTask::Fsync(table)));
    }

    waits
      .into_iter()
      .map(|done| done.wait().flatten())
      .collect::<Result>()?;

    Ok(())
  }
}
