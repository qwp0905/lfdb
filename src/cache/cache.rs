use std::{
  collections::{HashMap, VecDeque},
  mem::replace,
  sync::Arc,
};

use super::{
  Acquired, BatchHandle, BlockCell, BlockId, CachedBlock, CachedSlot, DirtyBlocks,
  DirtyTables, EvictionGuard, MappingTable, PendingFlush, RefedSlot,
};
use crate::{
  background::{Close, ThreadBuilder, ThreadPool},
  disk::{PagePool, Pointer, PAGE_SIZE},
  error, measure,
  metrics::MetricsRegistry,
  table::TableHandleRef,
  utils::{ExclusivePin, SharedToken, ToArc},
  Result,
};

pub struct BlockCacheConfig {
  pub shard_count: usize,
  pub capacity: usize,
  pub buffer_size: usize,
}

struct Core {
  cached_blocks: Box<[BlockCell]>,
  /**
   * pin to protect each block in cached blocks from eviction
   */
  pins: Box<[ExclusivePin]>,
  /**
   * each dirty bits are protected by each block's latch
   */
  dirty_blocks: DirtyBlocks,
  dirty_tables: DirtyTables,
}
impl Core {
  const fn new(
    cached_blocks: Box<[BlockCell]>,
    pins: Box<[ExclusivePin]>,
    dirty_blocks: DirtyBlocks,
    dirty_tables: DirtyTables,
  ) -> Self {
    Self {
      cached_blocks,
      pins,
      dirty_blocks,
      dirty_tables,
    }
  }

  const fn get_block_cell(&self, block_id: BlockId) -> &BlockCell {
    &self.cached_blocks[block_id]
  }
  const fn get_pin(&self, block_id: BlockId) -> &ExclusivePin {
    &self.pins[block_id]
  }
  const fn get_dirty_blocks(&self) -> &DirtyBlocks {
    &self.dirty_blocks
  }

  fn submit_eviction(&self, guard: &EvictionGuard) -> Option<PendingFlush> {
    if !guard.is_evicted() {
      return None;
    }
    let block_id = guard.get_block_id();
    if !self.dirty_blocks.contains(block_id) {
      return None;
    }

    Some(self.cached_blocks[block_id].get().flusher().submit())
  }

  fn resolve_eviction(
    &self,
    pending: Option<PendingFlush>,
    guard: &EvictionGuard,
    new: CachedBlock,
  ) -> Result {
    let block_id = guard.get_block_id();
    let block = &self.cached_blocks[block_id];

    if !guard.is_evicted() {
      block.write(new);
      return Ok(());
    }

    if let Some(pending) = pending {
      pending.finalize()?;
      self.dirty_blocks.remove(block_id);
      self.dirty_tables.mark(block.get().handle());
    }

    block.replace(new);
    Ok(())
  }

  fn flush_block(&self, id: BlockId) -> Result {
    let Some(_token) = self.pins[id].try_shared() else {
      return Ok(());
    };

    let block = self.cached_blocks[id].get();

    let flusher = block.exclusive_flusher();
    if !self.dirty_blocks.remove(id) {
      return Ok(());
    }

    let pending = flusher.submit();
    let (epoch, Err(err)) = pending.finalize() else {
      self.dirty_tables.mark(block.handle());
      return Ok(());
    };

    let latch = block.latch();
    if latch.epoch() == epoch {
      self.dirty_blocks.insert(id);
    }
    Err(err)
  }

  fn flush_tables_with(&self, executor: &Arc<ThreadPool>) -> Result {
    let mut stream = executor.stream(handle_flush_table);
    for table in self.dirty_tables.drain() {
      stream.push(table);
    }

    for (result, table) in stream.join() {
      let Err(err) = result else {
        continue;
      };

      // Preserve the unfinished fsync marker for a future caller. This flusher
      // reports the failure; retry policy belongs to the caller.
      error!("error occurs in flush table: {err}");
      self.dirty_tables.mark(&table);
      return Err(err);
    }
    Ok(())
  }

  fn aggregate_dirty_blocks(&self) -> Vec<BlockId> {
    let mut buckets = HashMap::<_, Vec<_>>::new();
    let mut len = 0;
    for id in self.dirty_blocks.iter() {
      let Some(_token) = self.pins[id].try_shared() else {
        // An exclusive owner is already handling this slot, for example during
        // eviction/install. Do not add it to this flush snapshot.
        continue;
      };

      // Snapshot dirty blocks into coarse disk-locality buckets. Flushing nearby
      // pointers from the same table together reduces random write scattering during
      // checkpoint.
      let block = self.cached_blocks[id].get();
      let key = (block.handle().get_id(), block.get_pointer() >> BUCKET_SHIFT);
      buckets.entry(key).or_default().push(id);
      len += 1;
    }

    let mut dirty = vec![0; len];
    let mut offset = 0;
    for bucket in buckets.into_values() {
      let end = offset + bucket.len();
      dirty[replace(&mut offset, end)..end].copy_from_slice(&bucket);
    }
    dirty
  }
}

/**
 * Central block-cache manager and higher-level disk abstraction.
 *
 * Upper storage layers access table pages through `BlockCache` rather than
 * through raw disk handles. The cache maps logical table blocks to cache slots,
 * loads missing pages, allocates fresh cached blocks, tracks dirty state, and
 * drives checkpoint flushing. `MappingTable` owns the logical-address to slot
 * mapping and eviction decisions; the block arrays, pins, batch handles, dirty
 * bitmap, and page pool hold the actual cached-page state.
 */
pub struct BlockCache {
  table: MappingTable,
  core: Arc<Core>,
  batch_handles: Box<[BatchHandle<RefedSlot>]>,
  page_pool: PagePool<PAGE_SIZE>,
  flush_executor: Arc<ThreadPool>,
  metrics: Arc<MetricsRegistry>,
}
impl BlockCache {
  pub fn open(config: BlockCacheConfig, metrics: Arc<MetricsRegistry>) -> Result<Self> {
    let page_pool = PagePool::new(config.capacity + config.buffer_size);

    let mut blocks = Vec::with_capacity(config.capacity);
    blocks.resize_with(config.capacity, BlockCell::uninit);

    let mut pins = Vec::with_capacity(config.capacity);
    pins.resize_with(config.capacity, ExclusivePin::new);

    let mut batch_handles = Vec::with_capacity(config.capacity);
    batch_handles.resize_with(config.capacity, BatchHandle::new);
    let batch_handles = batch_handles.into_boxed_slice();

    let core = Arc::new(Core::new(
      blocks.into_boxed_slice(),
      pins.into_boxed_slice(),
      DirtyBlocks::new(config.capacity),
      DirtyTables::new(),
    ));

    let flush_executor = ThreadBuilder::new()
      .name("flush executor")
      .multi(PRE_FLUSH_CONCURRENCY)
      .to_arc();

    Ok(Self {
      table: MappingTable::new(config.shard_count, config.capacity),
      core,
      batch_handles,
      page_pool,
      flush_executor,
      metrics,
    })
  }

  #[inline]
  fn cache_slot<'a>(&'a self, id: usize, token: SharedToken<'a>) -> CachedSlot<'a> {
    CachedSlot::new(
      self.core.get_block_cell(id).get(),
      self.core.get_dirty_blocks(),
      &self.batch_handles[id],
      id,
      token,
      &self.page_pool,
    )
  }

  /**
   * Allocate a cache slot for a newly allocated disk block.
   *
   * No disk read is performed because the logical block did not previously hold
   * meaningful contents. Callers must write the page contents before reading them.
   */
  pub fn alloc(
    &self,
    pointer: Pointer,
    handle: &TableHandleRef,
  ) -> Result<CachedSlot<'_>> {
    let table_id = handle.get_id();
    let guard = self
      .table
      .alloc(table_id, pointer, |id| self.core.get_pin(id));

    let pending = self.core.submit_eviction(&guard);
    let new_block = CachedBlock::new(pointer, self.page_pool.acquire(), handle.clone());
    self.resolve_eviction(pending, guard, new_block)
  }

  /**
   * Read a block through the unchecked disk-read path.
   *
   * This can tolerate an immediate EOF from the underlying file, unlike normal
   * reads. Use it only while reconstructing storage state before normal cache
   * invariants are established.
   */
  pub fn read_unchecked(
    &self,
    pointer: Pointer,
    handle: &TableHandleRef,
  ) -> Result<CachedSlot<'_>> {
    let table_id = handle.get_id();
    let guard = match self
      .table
      .acquire(table_id, pointer, |id| self.core.get_pin(id))
    {
      Acquired::Hit(block_id, token) => return Ok(self.cache_slot(block_id, token)),
      Acquired::Evicted(guard) => guard,
    };

    let pending = self.core.submit_eviction(&guard);
    let mut new = self.page_pool.acquire();
    handle.disk().read_unchecked(pointer, &mut new)?;
    let new_block = CachedBlock::new(pointer, new, handle.clone());
    self.resolve_eviction(pending, guard, new_block)
  }

  fn resolve_eviction<'a>(
    &'a self,
    pending: Option<PendingFlush>,
    guard: EvictionGuard<'a>,
    new: CachedBlock,
  ) -> Result<CachedSlot<'a>> {
    self.core.resolve_eviction(pending, &guard, new)?;
    Ok(self.cache_slot(guard.get_block_id(), guard.commit()))
  }

  fn __read(&self, pointer: Pointer, handle: &TableHandleRef) -> Result<CachedSlot<'_>> {
    let table_id = handle.get_id();
    let guard = match self
      .table
      .acquire(table_id, pointer, |id| &self.core.pins[id])
    {
      Acquired::Hit(block_id, token) => {
        self.metrics.block_cache_hit.inc();
        return Ok(self.cache_slot(block_id, token));
      }
      Acquired::Evicted(guard) => guard,
    };

    let pending = self.core.submit_eviction(&guard);
    let mut new = self.page_pool.acquire();
    handle.disk().read(pointer, &mut new)?;
    let new_block = CachedBlock::new(pointer, new, handle.clone());
    self.resolve_eviction(pending, guard, new_block)
  }

  #[inline]
  pub fn read(
    &self,
    pointer: Pointer,
    handle: &TableHandleRef,
  ) -> Result<CachedSlot<'_>> {
    measure!(self.metrics.block_cache_read, self.__read(pointer, handle))
  }

  pub fn create_flusher(&self) -> CacheFlusher {
    CacheFlusher::new(
      VecDeque::from(self.core.aggregate_dirty_blocks()),
      self.flush_executor.clone(),
      self.core.clone(),
    )
  }

  pub fn close(&self) {
    self.flush_executor.close();
  }
}

impl Drop for BlockCache {
  fn drop(&mut self) {
    for i in self
      .table
      .len_per_shard()
      .flat_map(|(len, offset)| offset..offset + len)
    {
      self.core.get_block_cell(i).drop_in_place();
    }
  }
}

/**
 * Coarse locality bucket used to order checkpoint writes by table and nearby
 * disk range.
 */
const FLUSH_BUCKET_PAGES: Pointer = (1 << 20) / PAGE_SIZE as Pointer; // 1Mib
const BUCKET_SHIFT: u32 = FLUSH_BUCKET_PAGES.ilog2();

/**
 * Incremental dirty-page flusher.
 *
 * A flusher owns a snapshot of dirty block ids. `advance` writes a bounded
 * number of dirty blocks so flushing can be spread across multiple calls.
 * `finish` completes the durability phase by fsyncing tables whose dirty pages
 * have been written.
 */
pub struct CacheFlusher {
  dirty_blocks: VecDeque<BlockId>,
  core: Arc<Core>,
  executor: Arc<ThreadPool>,
}
impl CacheFlusher {
  const fn new(
    dirty_blocks: VecDeque<BlockId>,
    executor: Arc<ThreadPool>,
    core: Arc<Core>,
  ) -> Self {
    Self {
      dirty_blocks,
      executor,
      core,
    }
  }

  pub fn advance(&mut self, count: usize) -> Result {
    let count = count.min(self.dirty_blocks.len());
    let handler = handle_flush_blocks(self.core.clone());
    let tasks = self.dirty_blocks.iter().take(count).copied();

    self
      .executor
      .fork(tasks, handler)
      .join()
      .collect::<Result>()?;
    self.dirty_blocks.drain(..count);

    Ok(())
  }

  pub fn flush_hard(&mut self) -> Result {
    self.advance(self.remaining())?;
    self.finish()
  }

  pub fn remaining(&self) -> usize {
    self.dirty_blocks.len()
  }

  pub fn is_done(&self) -> bool {
    self.dirty_blocks.is_empty()
  }

  pub fn finish(&self) -> Result {
    self.core.flush_tables_with(&self.executor)
  }
}

const PRE_FLUSH_CONCURRENCY: usize = 3;

const fn handle_flush_blocks(core: Arc<Core>) -> impl Fn(BlockId) -> Result {
  move |id| core.flush_block(id)
}

fn handle_flush_table(table: TableHandleRef) -> (Result, TableHandleRef) {
  (table.disk().fsync(), table)
}
