use std::{
  cell::UnsafeCell,
  collections::{HashMap, VecDeque},
  mem::{replace, MaybeUninit},
  sync::Arc,
};

use super::{
  Acquired, BatchHandle, BlockId, CachedBlock, CachedSlot, DirtyTables, EvictionGuard,
  MappingTable,
};
use crate::{
  background::{BackgroundThread, ThreadBuilder},
  disk::{PagePool, Pointer, PAGE_SIZE},
  error, measure,
  metrics::MetricsRegistry,
  table::TableHandleRef,
  utils::{AtomicBitmap, ExclusivePin, SharedToken, ToArc},
  Result,
};

pub struct BlockCacheConfig {
  pub shard_count: usize,
  pub capacity: usize,
}

/**
 * Lazily initialized cache-slot storage.
 *
 * The cache allocates the slot array up front, but a `CachedBlock` is only
 * written into a slot once the mapping table assigns that slot. Drop therefore
 * only visits the initialized ranges reported by the mapping table.
 */
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
  flush_executor: Arc<BackgroundThread<FlushTask, Result>>,
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

    let flush_executor = ThreadBuilder::new()
      .name("flush executor")
      .multi(PRE_FLUSH_CONCURRENCY)
      .shared(handle_execute(
        cached_blocks.clone(),
        pins.clone(),
        dirty_blocks.clone(),
        dirty_tables.clone(),
      ))
      .to_arc();

    Ok(Self {
      cached_blocks,
      pins,
      batch_handles,
      table: MappingTable::new(config.shard_count, block_cap),
      dirty_blocks,
      page_pool,
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
    let guard = self.table.alloc(table_id, pointer, |id| &self.pins[id]);

    let new_block = CachedBlock::new(pointer, self.page_pool.acquire(), handle.clone());
    self.handle_eviction(guard, new_block)
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
    let guard = match self.table.acquire(table_id, pointer, |id| &self.pins[id]) {
      Acquired::Hit(block_id, token) => return Ok(self.cache_slot(block_id, token)),
      Acquired::Evicted(guard) => guard,
    };

    let mut new = self.page_pool.acquire();
    handle.disk().read_unchecked(pointer, &mut new)?;
    let new_block = CachedBlock::new(pointer, new, handle.clone());
    self.handle_eviction(guard, new_block)
  }

  fn __read(&self, pointer: Pointer, handle: &TableHandleRef) -> Result<CachedSlot<'_>> {
    let table_id = handle.get_id();
    let guard = match self.table.acquire(table_id, pointer, |id| &self.pins[id]) {
      Acquired::Hit(block_id, token) => {
        self.metrics.block_cache_hit.inc();
        return Ok(self.cache_slot(block_id, token));
      }
      Acquired::Evicted(guard) => guard,
    };

    let mut new = self.page_pool.acquire();
    handle.disk().read(pointer, &mut new)?;
    let new_block = CachedBlock::new(pointer, new, handle.clone());
    self.handle_eviction(guard, new_block)
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

    CacheFlusher::new(
      VecDeque::from(dirty),
      self.flush_executor.clone(),
      self.dirty_tables.clone(),
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
      self.cached_blocks[i].drop_in_place();
    }
  }
}

/**
 * Coarse locality bucket used to order checkpoint writes by table and nearby
 * disk range.
 */
const FLUSH_BUCKET_PAGES: Pointer = (1 << 20) / PAGE_SIZE as Pointer; // 1Mib
const BUCKET_SHIFT: Pointer = FLUSH_BUCKET_PAGES.ilog2() as Pointer;

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
  executor: Arc<BackgroundThread<FlushTask, Result>>,
  dirty_tables: Arc<DirtyTables>,
}
impl CacheFlusher {
  const fn new(
    dirty_blocks: VecDeque<BlockId>,
    executor: Arc<BackgroundThread<FlushTask, Result>>,
    dirty_tables: Arc<DirtyTables>,
  ) -> Self {
    Self {
      dirty_blocks,
      executor,
      dirty_tables,
    }
  }

  pub fn advance(&mut self, count: usize) -> Result {
    let count = count.min(self.dirty_blocks.len());
    let mut waiting = Vec::with_capacity(count);
    for &id in self.dirty_blocks.iter().take(count) {
      waiting.push(self.executor.execute(FlushTask::Write(id)));
    }
    waiting
      .into_iter()
      .try_for_each(|done| done.wait().unwrap())?;
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
    let mut waiting = Vec::new();
    for table in self.dirty_tables.drain() {
      let done = self.executor.execute(FlushTask::Fsync(table.clone()));
      waiting.push((table, done));
    }

    for (table, done) in waiting {
      let Err(err) = done.wait().unwrap() else {
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
}

const PRE_FLUSH_CONCURRENCY: usize = 4;

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
      let Some(_token) = pins[id].try_shared() else {
        return Ok(());
      };

      let block = blocks[id].get();
      let flusher = block.flusher();
      if !dirty_blocks.remove(id) {
        return Ok(());
      }

      let result = flusher.submit();
      let (epoch, Err(err)) = result.finalize() else {
        dirty_tables.mark(block.handle());
        return Ok(());
      };

      let latch = block.latch();
      if latch.epoch() == epoch {
        dirty_blocks.insert(id);
      }
      Err(err)
    }
    FlushTask::Fsync(table) => table.disk().fsync(),
  }
}
