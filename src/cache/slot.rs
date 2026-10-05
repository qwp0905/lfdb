use std::mem::ManuallyDrop;

use super::{BlockId, BlockLatch, CachedBlock, DirtyBlocks};
use crate::{
  disk::Pointer,
  page::{Page, PageAllocator, PageRef, PAGE_SIZE},
  utils::{SBox, SharedToken},
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
  token: SharedToken<'a>,
  page_pool: &'a PageAllocator<PAGE_SIZE>,
}
impl<'a> CachedSlot<'a> {
  pub fn new(
    block: &'a CachedBlock,
    dirty: &'a DirtyBlocks,
    block_id: BlockId,
    token: SharedToken<'a>,
    page_pool: &'a PageAllocator<PAGE_SIZE>,
  ) -> Self {
    Self {
      block,
      dirty,
      block_id,
      token,
      page_pool,
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
      state: CopiedState::Borrowed {
        page: self.block.load_page(),
        dirty_blocks: self.dirty,
        page_pool: self.page_pool,
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
    page: SBox<PageRef<PAGE_SIZE>>,
    dirty_blocks: &'a DirtyBlocks,
    page_pool: &'a PageAllocator<PAGE_SIZE>,
    block_id: BlockId,
  },
  Copied(ManuallyDrop<PageRef<PAGE_SIZE>>),
}

pub struct WritableSlot<'a> {
  pointer: Pointer,
  state: CopiedState<'a>,
  latch: BlockLatch<'a>,
  _token: SharedToken<'a>,
}
impl<'a> WritableSlot<'a> {
  pub const fn get_pointer(&self) -> Pointer {
    self.pointer
  }

  pub fn modify_with<T, F: FnOnce(&mut Page) -> T>(&mut self, f: F) -> T {
    match &mut self.state {
      CopiedState::Borrowed {
        page: _,
        dirty_blocks,
        page_pool,
        block_id,
      } => {
        let mut page = page_pool.allocate();
        let result = f(&mut page);
        dirty_blocks.insert(*block_id);
        self.state = CopiedState::Copied(ManuallyDrop::new(page));
        result
      }
      CopiedState::Copied(shadow) => f(shadow),
    }
  }
  pub fn copy_from(&mut self, data: &[u8]) {
    self.modify_with(|page| page.copy_from(data, 0))
  }
}
impl<'a> AsRef<Page> for WritableSlot<'a> {
  fn as_ref(&self) -> &Page {
    match &self.state {
      CopiedState::Borrowed { page, .. } => page,
      CopiedState::Copied(shadow) => shadow,
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
    self.latch.apply(shadow);
  }
}
