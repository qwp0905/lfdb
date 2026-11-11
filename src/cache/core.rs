use std::{collections::HashMap, mem::replace, sync::Arc};

use crate::{
  disk::Pointer,
  page::{PageAllocator, PageRef, PAGE_SIZE},
  table::TableHandleRef,
  wal::{WriteAheadLog, WriteSeq},
  Result,
};

use super::{
  BlockId, CachedBlock, DirtyTables, EvictionGuard, MappingTable, PendingFlush,
};

/**
 * Coarse locality bucket used to order checkpoint writes by table and nearby
 * disk range.
 */
const FLUSH_BUCKET_PAGES: Pointer = (1 << 20) / PAGE_SIZE as Pointer; // 1Mib
const BUCKET_SHIFT: u32 = FLUSH_BUCKET_PAGES.ilog2();

pub struct BlockCacheCore {
  mapping: MappingTable,
  cached_blocks: Box<[CachedBlock]>,
  wal: Arc<WriteAheadLog>,
  dirty_tables: DirtyTables,
  allocator: PageAllocator<PAGE_SIZE>,
}
impl BlockCacheCore {
  pub const fn new(
    mapping: MappingTable,
    cached_blocks: Box<[CachedBlock]>,
    wal: Arc<WriteAheadLog>,
    dirty_tables: DirtyTables,
    allocator: PageAllocator<PAGE_SIZE>,
  ) -> Self {
    Self {
      mapping,
      cached_blocks,
      wal,
      dirty_tables,
      allocator,
    }
  }

  pub fn mark_dirty_table(&self, table: &TableHandleRef) {
    self.dirty_tables.mark(table);
  }

  pub fn ensure_durability(&self, write_seq: WriteSeq) -> Result {
    self.wal.flush_until(write_seq)
  }

  pub const fn get_mapping(&self) -> &'_ MappingTable {
    &self.mapping
  }

  pub const fn get_block(&self, block_id: BlockId) -> &CachedBlock {
    &self.cached_blocks[block_id]
  }

  pub fn submit_eviction(&self, guard: &EvictionGuard) -> Result<Option<PendingFlush>> {
    if !guard.is_evicted() {
      return Ok(None);
    }
    let block = &self.cached_blocks[guard.get_block_id()];
    let Some(seq) = block.get_dirty() else {
      return Ok(None);
    };
    self.wal.flush_until(seq)?;
    Ok(Some(block.submit_flush()))
  }

  pub fn resolve_eviction(
    &self,
    pending: Option<PendingFlush>,
    guard: &EvictionGuard,
    new: PageRef<PAGE_SIZE>,
    pointer: Pointer,
    table: TableHandleRef,
  ) -> Result {
    let block = &self.cached_blocks[guard.get_block_id()];

    if !guard.is_evicted() {
      unsafe { block.init(new, pointer, table) };
      return Ok(());
    }

    if let Some(pending) = pending {
      pending.finalize()?;
      self.mark_dirty_table(block.handle());
      block.clear_dirty();
    }

    unsafe { block.replace(new, pointer, table) };
    Ok(())
  }

  pub fn flush_block(&self, id: BlockId) -> Result {
    let Some(_token) = self.mapping.try_pin(id) else {
      return Ok(());
    };

    let block = &self.cached_blocks[id];
    let Some(snapshot) = block.flush_dirty() else {
      return Ok(());
    };
    self.wal.flush_until(snapshot.get_write_seq())?;
    snapshot.submit().finalize()?;
    self.mark_dirty_table(block.handle());
    Ok(())
  }

  pub fn flush_tables(&self) -> Result {
    let mut pending = Vec::new();
    for table in self.dirty_tables.drain() {
      pending.push((table.disk().fsync(), table));
    }

    let mut failed = None;
    for (pending, table) in pending {
      let Err(err) = pending.wait_flatten() else {
        continue;
      };

      failed
        .get_or_insert_with(|| (err, Vec::new()))
        .1
        .push(table);
    }
    let Some((err, failed)) = failed else {
      return Ok(());
    };
    for table in failed {
      self.mark_dirty_table(&table);
    }
    Err(err)
  }

  pub fn aggregate_dirty_blocks(&self) -> Vec<BlockId> {
    let mut buckets = HashMap::<_, Vec<_>>::new();
    let mut len = 0;

    for (id, block) in self
      .cached_blocks
      .iter()
      .enumerate()
      .filter(|(_, block)| block.get_dirty().is_some())
    {
      let Some(_token) = self.mapping.try_pin(id) else {
        // An exclusive owner is already handling this slot, for example during
        // eviction/install. Do not add it to this flush snapshot.
        continue;
      };

      // Snapshot dirty blocks into coarse disk-locality buckets. Flushing nearby
      // pointers from the same table together reduces random write scattering during
      // checkpoint.
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

  pub fn allocate_page(&self) -> PageRef<PAGE_SIZE> {
    self.allocator.allocate()
  }

  pub fn acquire_page_with(&self, guard: &EvictionGuard) -> PageRef<PAGE_SIZE> {
    if guard.is_evicted() {
      self.allocator.allocate()
    } else {
      self.allocator.allocate_new()
    }
  }
}
