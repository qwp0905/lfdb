use std::ops::Range;

use super::{BlockId, BlockLatch, CachedBlock, DirtyBlocks, MappingToken};
use crate::{
  cache::dirty::DirtyTables,
  disk::Pointer,
  page::{Page, PageAllocator, PageRef, VecRef, PAGE_SIZE},
  table::TableHandleRef,
  utils::{create_static_ref, SBox},
  Result,
};

/**
 * Access interface for one cached block.
 *
 * A `CachedSlot` is returned after the block cache has found and pinned a block.
 * The caller then chooses the access mode: read the current page, write through
 * a shadow page, or join a batched mutation pass. The slot hides the cached page
 * replacement, dirty marking, and page-pool details behind those modes.
 */
pub struct CachedSlot<'a> {
  block: &'a CachedBlock,
  dirty: &'a DirtyBlocks,
  block_id: BlockId,
  token: MappingToken<'a>,
  allocator: &'a PageAllocator<PAGE_SIZE>,
}
impl<'a> CachedSlot<'a> {
  pub const fn new(
    block: &'a CachedBlock,
    dirty: &'a DirtyBlocks,
    block_id: BlockId,
    token: MappingToken<'a>,
    allocator: &'a PageAllocator<PAGE_SIZE>,
  ) -> Self {
    Self {
      block,
      dirty,
      block_id,
      token,
      allocator,
    }
  }

  pub fn for_read(self) -> ReadonlySlot {
    ReadonlySlot {
      page: self.block.load_page(),
    }
  }
  pub fn for_write<'b>(self) -> WritableSlot<'b>
  where
    'a: 'b,
  {
    let latch = self.block.latch();
    WritableSlot {
      pointer: self.block.get_pointer(),
      target: WriteTarget::Cached {
        page: self.block.load_page(),
        dirty_blocks: self.dirty,
        allocator: self.allocator,
        block_id: self.block_id,
        latch,
        _token: self.token,
      },
    }
  }
}

/**
 * Immutable snapshot of a cached page.
 *
 * The slot owns an `SBox` reference to the page version it loaded. Later writers
 * may replace the block's current page, but this reader continues to observe the
 * same page snapshot without batch mutation.
 */
pub struct ReadonlySlot {
  page: SBox<PageRef<PAGE_SIZE>>,
}
impl ReadonlySlot {
  pub fn create_ref(&self, range: Range<usize>) -> VecRef {
    VecRef::refed(self.page.clone(), range)
  }
}
impl AsRef<Page<PAGE_SIZE>> for ReadonlySlot {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    &self.page
  }
}

enum WriteTarget<'a> {
  Cached {
    page: SBox<PageRef<PAGE_SIZE>>,
    dirty_blocks: &'a DirtyBlocks,
    allocator: &'a PageAllocator<PAGE_SIZE>,
    block_id: BlockId,
    latch: BlockLatch<'a>,
    _token: MappingToken<'a>,
  },
  Temp {
    page: PageRef<PAGE_SIZE>,
    table: TableHandleRef,
    dirty_tables: &'a DirtyTables,
  },
}

/**
 * A page is changed only inside `commit_with`, and the change is installed only
 * when the closure succeeds, so a page whose serialization or WAL append failed
 * never reaches the cache or the disk.
 */
pub struct WritableSlot<'a> {
  pointer: Pointer,
  target: WriteTarget<'a>,
}
impl<'a> WritableSlot<'a> {
  pub const fn new_temp(
    table: TableHandleRef,
    page: PageRef<PAGE_SIZE>,
    pointer: Pointer,
    dirty_tables: &'a DirtyTables,
  ) -> Self {
    Self {
      pointer,
      target: WriteTarget::Temp {
        page,
        table,
        dirty_tables,
      },
    }
  }

  pub const fn get_pointer(&self) -> Pointer {
    self.pointer
  }

  pub fn commit_with<T, F>(self, f: F) -> Result<T>
  where
    F: FnOnce(&mut Page<PAGE_SIZE>) -> Result<T>,
  {
    match self.target {
      WriteTarget::Cached {
        dirty_blocks,
        allocator,
        block_id,
        mut latch,
        _token,
        ..
      } => {
        let mut shadow = allocator.allocate();
        let result = f(&mut shadow)?;
        dirty_blocks.insert(block_id);
        latch.apply(shadow);
        Ok(result)
      }
      WriteTarget::Temp {
        mut page,
        table,
        dirty_tables,
      } => {
        let result = f(&mut page)?;
        let static_ref = unsafe { create_static_ref::<Page<PAGE_SIZE>>(&page) };
        table
          .disk()
          .write_async(self.pointer, static_ref)
          .wait_flatten()?;
        dirty_tables.mark(&table);
        Ok(result)
      }
    }
  }
}
impl<'a> AsRef<Page<PAGE_SIZE>> for WritableSlot<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    match &self.target {
      WriteTarget::Cached { page, .. } => page,
      WriteTarget::Temp { page, .. } => page,
    }
  }
}

unsafe impl<'a> Send for WritableSlot<'a> {}
unsafe impl<'a> Sync for WritableSlot<'a> {}
