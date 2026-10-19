use std::{mem::ManuallyDrop, ops::Range};

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
      state: CopiedState::Borrowed {
        pointer: self.block.get_pointer(),
        page: self.block.load_page(),
        dirty_blocks: self.dirty,
        allocator: self.allocator,
        block_id: self.block_id,
      },
      latch,
      _token: self.token,
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
impl Clone for ReadonlySlot {
  fn clone(&self) -> Self {
    Self {
      page: self.page.clone(),
    }
  }
}

enum CopiedState<'a> {
  Borrowed {
    pointer: Pointer,
    page: SBox<PageRef<PAGE_SIZE>>,
    dirty_blocks: &'a DirtyBlocks,
    allocator: &'a PageAllocator<PAGE_SIZE>,
    block_id: BlockId,
  },
  Copied(ManuallyDrop<PreparedSlot>),
}

pub struct WritableSlot<'a> {
  state: CopiedState<'a>,
  latch: BlockLatch<'a>,
  _token: MappingToken<'a>,
}
impl<'a> WritableSlot<'a> {
  pub fn get_pointer(&self) -> Pointer {
    match &self.state {
      CopiedState::Borrowed { pointer, .. } => *pointer,
      CopiedState::Copied(slot) => slot.get_pointer(),
    }
  }
  pub fn as_prepared(&mut self) -> &mut PreparedSlot {
    if let CopiedState::Borrowed {
      pointer,
      page: _,
      dirty_blocks,
      allocator,
      block_id,
    } = &self.state
    {
      let page = allocator.allocate();
      dirty_blocks.insert(*block_id);
      self.state =
        CopiedState::Copied(ManuallyDrop::new(PreparedSlot::new(*pointer, page)));
    };

    let CopiedState::Copied(slot) = &mut self.state else {
      unreachable!();
    };
    slot
  }
}
impl<'a> AsRef<Page<PAGE_SIZE>> for WritableSlot<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    match &self.state {
      CopiedState::Borrowed { page, .. } => page,
      CopiedState::Copied(slot) => slot.as_ref(),
    }
  }
}

unsafe impl<'a> Send for WritableSlot<'a> {}
unsafe impl<'a> Sync for WritableSlot<'a> {}

impl<'a> Drop for WritableSlot<'a> {
  fn drop(&mut self) {
    let shadow = match &mut self.state {
      CopiedState::Borrowed { .. } => return,
      CopiedState::Copied(shadow) => unsafe { ManuallyDrop::take(shadow) },
    };
    self.latch.apply(shadow.into_inner());
  }
}

pub struct PreparedSlot {
  pointer: Pointer,
  page: PageRef<PAGE_SIZE>,
}
impl PreparedSlot {
  const fn new(pointer: Pointer, page: PageRef<PAGE_SIZE>) -> Self {
    Self { pointer, page }
  }
  pub const fn get_pointer(&self) -> Pointer {
    self.pointer
  }
  pub fn into_inner(self) -> PageRef<PAGE_SIZE> {
    self.page
  }
}
impl AsRef<Page> for PreparedSlot {
  fn as_ref(&self) -> &Page {
    &self.page
  }
}
impl AsMut<Page> for PreparedSlot {
  fn as_mut(&mut self) -> &mut Page {
    &mut self.page
  }
}

pub enum AllocatedSlot<'a> {
  Hit(WritableSlot<'a>),
  Temp(TempSlot<'a>),
}
impl<'a> AllocatedSlot<'a> {
  pub const fn new_temp(
    table: TableHandleRef,
    page: PageRef<PAGE_SIZE>,
    pointer: Pointer,
    dirty_tables: &'a DirtyTables,
  ) -> Self {
    Self::Temp(TempSlot::new(table, page, pointer, dirty_tables))
  }
  pub fn finalize(&mut self) -> Result {
    match self {
      Self::Hit(_) => Ok(()),
      Self::Temp(temp) => temp.finalize(),
    }
  }
  pub fn get_pointer(&self) -> Pointer {
    match self {
      Self::Hit(slot) => slot.get_pointer(),
      Self::Temp(slot) => slot.refed.get_pointer(),
    }
  }

  pub fn as_prepared(&mut self) -> &mut PreparedSlot {
    match self {
      Self::Hit(slot) => slot.as_prepared(),
      Self::Temp(slot) => &mut slot.refed,
    }
  }
}

pub struct TempSlot<'a> {
  table: TableHandleRef,
  refed: PreparedSlot,
  dirty_tables: &'a DirtyTables,
}
impl<'a> TempSlot<'a> {
  const fn new(
    table: TableHandleRef,
    page: PageRef<PAGE_SIZE>,
    pointer: Pointer,
    dirty_tables: &'a DirtyTables,
  ) -> Self {
    Self {
      table,
      refed: PreparedSlot::new(pointer, page),
      dirty_tables,
    }
  }
  fn finalize(&mut self) -> Result {
    let static_ref = unsafe { create_static_ref::<Page<PAGE_SIZE>>(self.refed.as_ref()) };
    self
      .table
      .disk()
      .write_async(self.refed.get_pointer(), static_ref)
      .wait_flatten()?;
    self.dirty_tables.mark(&self.table);
    Ok(())
  }
}
