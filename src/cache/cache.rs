use std::{collections::VecDeque, sync::Arc};

use super::{
  Acquired, BlockCacheCore, BlockId, CachedBlock, CachedSlot, DirtyTables, EvictionGuard,
  MappingTable, PendingFlush, WritableSlot,
};
use crate::{
  background::{SharedWorkThread, ThreadBuilder},
  disk::Pointer,
  metrics::{measure, MetricsRegistry},
  page::{PageAllocator, PageRef, PAGE_SIZE},
  table::TableHandleRef,
  utils::ToArc,
  wal::{WriteAheadLog, WriteSeq},
  Result,
};

pub struct BlockCacheConfig {
  pub shard_count: usize,
  pub capacity: usize,
  pub buffer_size: usize,
}

/**
 * Central block-cache manager and higher-level disk abstraction.
 *
 * Upper storage layers access table pages through `BlockCache` rather than
 * through raw disk handles. The cache maps logical table blocks to cache slots,
 * loads missing pages, allocates fresh cached blocks, tracks dirty state, and
 * drives checkpoint flushing. `MappingTable` owns the logical-address to slot
 * mapping and eviction decisions, and `BlockCacheCore` owns the cached blocks and
 * writes a page only after its log record is durable.
 */
pub struct BlockCache {
  core: Arc<BlockCacheCore>,
  flush_executor: Arc<SharedWorkThread<(BlockId, WriteSeq), Result>>,
  metrics: Arc<MetricsRegistry>,
}
impl BlockCache {
  pub fn open(
    config: BlockCacheConfig,
    wal: Arc<WriteAheadLog>,
    metrics: Arc<MetricsRegistry>,
  ) -> Self {
    let capacity = config.capacity / PAGE_SIZE;
    let buffer_size = config.buffer_size / PAGE_SIZE;
    let allocator = PageAllocator::new(buffer_size);

    let mut blocks = Vec::with_capacity(capacity);
    blocks.resize_with(capacity, CachedBlock::uninit);

    let core = Arc::new(BlockCacheCore::new(
      MappingTable::new(config.shard_count, capacity),
      blocks.into_boxed_slice(),
      wal,
      DirtyTables::new(),
      allocator,
    ));

    let flush_executor = ThreadBuilder::new()
      .name("flush executor")
      .multi(PRE_FLUSH_CONCURRENCY)
      .shared(handle_flush_blocks(core.clone()))
      .to_arc();
    Self {
      core,
      flush_executor,
      metrics,
    }
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
  ) -> Result<WritableSlot<'_>> {
    let table_id = handle.get_id();
    let Some(acquired) = self.core.get_mapping().try_acquire(table_id, pointer) else {
      return Ok(WritableSlot::new_temp(
        handle.clone(),
        self.core.allocate_page(),
        pointer,
        &self.core,
      ));
    };
    let guard = match acquired {
      Acquired::Hit(block_id, token) => {
        return Ok(CachedSlot::new(block_id, &self.core, token).for_write());
      }
      Acquired::Evicted(guard) => guard,
    };
    let pending = self.core.submit_eviction(&guard)?;
    let new = self.core.acquire_page_with(&guard);
    let slot = self.resolve_eviction(pending, guard, new, pointer, handle.clone())?;
    Ok(slot.for_write())
  }

  /**
   * Read a block through the unchecked disk-read path.
   *
   * This can tolerate an immediate EOF from the underlying file, unlike normal
   * reads. Use it only while reconstructing storage state before normal cache
   * invariants are established.
   */
  pub unsafe fn read_unchecked(
    &self,
    pointer: Pointer,
    handle: &TableHandleRef,
  ) -> Result<CachedSlot<'_>> {
    let table_id = handle.get_id();
    let guard = match self.core.get_mapping().acquire(table_id, pointer) {
      Acquired::Hit(block_id, token) => {
        return Ok(CachedSlot::new(block_id, &self.core, token))
      }
      Acquired::Evicted(guard) => guard,
    };

    let pending = self.core.submit_eviction(&guard)?;
    let mut new = self.core.acquire_page_with(&guard);
    unsafe { handle.disk().read_unchecked(pointer, &mut new)? };
    self.resolve_eviction(pending, guard, new, pointer, handle.clone())
  }

  fn resolve_eviction<'a>(
    &'a self,
    pending: Option<PendingFlush>,
    guard: EvictionGuard<'a>,
    new: PageRef<PAGE_SIZE>,
    pointer: Pointer,
    table: TableHandleRef,
  ) -> Result<CachedSlot<'a>> {
    self
      .core
      .resolve_eviction(pending, &guard, new, pointer, table)?;
    Ok(CachedSlot::new(
      guard.get_block_id(),
      &self.core,
      guard.commit(),
    ))
  }

  fn __read(&self, pointer: Pointer, handle: &TableHandleRef) -> Result<CachedSlot<'_>> {
    let table_id = handle.get_id();
    let guard = match self.core.get_mapping().acquire(table_id, pointer) {
      Acquired::Hit(block_id, token) => {
        self.metrics.block_cache_hit.inc();
        return Ok(CachedSlot::new(block_id, &self.core, token));
      }
      Acquired::Evicted(guard) => guard,
    };

    let pending = self.core.submit_eviction(&guard)?;
    let mut new = self.core.acquire_page_with(&guard);
    handle.disk().read(pointer, &mut new)?;
    self.resolve_eviction(pending, guard, new, pointer, handle.clone())
  }

  #[inline]
  pub fn read(
    &self,
    pointer: Pointer,
    handle: &TableHandleRef,
  ) -> Result<CachedSlot<'_>> {
    measure!(self.metrics.block_cache_read, self.__read(pointer, handle))
  }

  pub fn create_flusher(&self, boundary: WriteSeq) -> Result<CacheFlusher> {
    let dirty_blocks = self.core.aggregate_dirty_blocks(boundary);
    if !dirty_blocks.is_empty() {
      self.core.ensure_durability(boundary - 1)?;
    }
    Ok(CacheFlusher::new(
      VecDeque::from(dirty_blocks),
      boundary,
      self.flush_executor.clone(),
      self.core.clone(),
    ))
  }

  pub fn close(&self) {
    self.flush_executor.close();
  }
}

impl Drop for BlockCache {
  fn drop(&mut self) {
    for i in self.core.get_mapping().get_block_ids() {
      unsafe { self.core.get_block(i).drop_in_place() };
    }
  }
}
/**
 * Incremental dirty-page flusher.
 *
 * A flusher owns a snapshot of dirty block ids. `advance` writes a bounded
 * number of dirty blocks so flushing can be spread across multiple calls.
 * `finish` completes the durability phase by fsync tables whose dirty pages
 * have been written.
 */
pub struct CacheFlusher {
  dirty_blocks: VecDeque<BlockId>,
  boundary: WriteSeq,
  core: Arc<BlockCacheCore>,
  executor: Arc<SharedWorkThread<(BlockId, WriteSeq), Result>>,
}
impl CacheFlusher {
  const fn new(
    dirty_blocks: VecDeque<BlockId>,
    boundary: WriteSeq,
    executor: Arc<SharedWorkThread<(BlockId, WriteSeq), Result>>,
    core: Arc<BlockCacheCore>,
  ) -> Self {
    Self {
      dirty_blocks,
      boundary,
      executor,
      core,
    }
  }

  pub fn advance(&mut self, count: usize) -> Result {
    let count = count.min(self.dirty_blocks.len());
    let mut pending = Vec::with_capacity(count);
    for &id in self.dirty_blocks.iter().take(count) {
      pending.push(self.executor.execute((id, self.boundary)));
    }

    for pending in pending {
      pending.wait().unwrap()?;
    }
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
    self.core.flush_tables()
  }
}

const PRE_FLUSH_CONCURRENCY: usize = 3;

const fn handle_flush_blocks(
  core: Arc<BlockCacheCore>,
) -> impl Fn((BlockId, WriteSeq)) -> Result {
  move |(id, boundary)| core.flush_block(id, boundary)
}

#[cfg(all(test, not(loom)))]
#[path = "tests/cache.rs"]
mod tests;
