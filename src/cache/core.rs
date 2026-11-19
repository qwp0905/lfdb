use std::{
  collections::HashMap,
  mem::{replace, ManuallyDrop},
  sync::Arc,
};

use crate::{
  background::{Selectable, TryWaitError},
  disk::{PendingIO, Pointer},
  page::{PageAllocator, PageRef, PAGE_SIZE},
  table::TableHandleRef,
  wal::{WriteAheadLog, WriteSeq},
  Error, Result,
};

use super::{
  BlockId, CachedBlock, DirtyTableRef, DirtyTables, EvictionGuard, MappingTable,
  MappingToken, PendingDirty, PendingDiskWrite,
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

  pub fn submit_eviction(
    &self,
    guard: &EvictionGuard,
  ) -> Result<Option<PendingDiskWrite>> {
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
    pending: Option<PendingDiskWrite>,
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

  pub fn submit_flush_block(
    &self,
    id: BlockId,
    boundary: WriteSeq,
  ) -> Option<PendingFlushBlock<'_>> {
    let token = self.mapping.try_pin(id)?;

    // A block rewritten at or after the boundary is left dirty: its newest record
    // holds the whole page, so replay from the boundary restores it, and the
    // checkpoint record is flushed after that record.
    let block = &self.cached_blocks[id];
    let snapshot = block.flush_dirty(boundary)?;
    Some(PendingFlushBlock::new(
      token,
      snapshot.submit(),
      block.handle(),
      &self.dirty_tables,
    ))
  }

  pub fn flush_tables(&self) -> impl Iterator<Item = PendingFlushTable<'_>> + '_ {
    self.dirty_tables.get_all().map(PendingFlushTable::new)
  }

  pub fn aggregate_dirty_blocks(&self, boundary: WriteSeq) -> Vec<BlockId> {
    let mut buckets = HashMap::<_, Vec<_>>::new();
    let mut len = 0;

    for (id, block) in self
      .cached_blocks
      .iter()
      .enumerate()
      .filter(|(_, block)| block.get_dirty().is_some_and(|seq| seq < boundary))
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

pub struct PendingFlushBlock<'a> {
  pending: ManuallyDrop<PendingDirty<'a>>,
  token: ManuallyDrop<MappingToken<'a>>,
  table: &'a TableHandleRef,
  dirty: &'a DirtyTables,
}
impl<'a> PendingFlushBlock<'a> {
  const fn new(
    token: MappingToken<'a>,
    pending: PendingDirty<'a>,
    table: &'a TableHandleRef,
    dirty: &'a DirtyTables,
  ) -> Self {
    Self {
      token: ManuallyDrop::new(token),
      pending: ManuallyDrop::new(pending),
      table,
      dirty,
    }
  }
}
impl<'a> Drop for PendingFlushBlock<'a> {
  fn drop(&mut self) {
    unsafe { ManuallyDrop::drop(&mut self.pending) };
    unsafe { ManuallyDrop::drop(&mut self.token) };
  }
}
impl<'a> Selectable for PendingFlushBlock<'a> {
  type Output = Result;

  fn try_wait(self) -> std::result::Result<Self::Output, TryWaitError<Self>> {
    let mut this = ManuallyDrop::new(self);
    let token = unsafe { ManuallyDrop::take(&mut this.token) };
    let pending = unsafe { ManuallyDrop::take(&mut this.pending) };
    let result = match pending.try_finalize() {
      Ok(result) => result,
      Err(err) => {
        return Err(
          err.map_empty(|pending| Self::new(token, pending, this.table, this.dirty)),
        )
      }
    };
    if let Err(err) = result {
      return Ok(Err(err));
    }
    this.dirty.mark(this.table);
    Ok(Ok(()))
  }

  fn try_park(&self) -> bool {
    self.pending.try_park()
  }
}

pub struct PendingFlushTable<'a> {
  pending: PendingIO,
  table: DirtyTableRef<'a>,
}
impl<'a> PendingFlushTable<'a> {
  fn new(table: DirtyTableRef<'a>) -> Self {
    let pending = table.as_table().disk().fsync();
    Self { pending, table }
  }
}
impl<'a> Selectable for PendingFlushTable<'a> {
  type Output = Result;

  fn try_wait(self) -> std::result::Result<Self::Output, TryWaitError<Self>> {
    let result = match self.pending.try_wait() {
      Ok(result) => result,
      Err(err) => {
        return Err(err.map_empty(|pending| Self {
          pending,
          table: self.table,
        }))
      }
    };
    if let Err(err) = result {
      return Ok(Err(Error::IO(err)));
    }
    self.table.finish();
    Ok(Ok(()))
  }

  fn try_park(&self) -> bool {
    self.pending.try_park()
  }
}
