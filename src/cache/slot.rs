use std::{
  mem::ManuallyDrop,
  ops::{Deref, DerefMut},
  ptr::NonNull,
};

use super::{BlockId, BlockLatch, CachedBlock, DirtyBlocks};
use crate::{
  disk::{Page, PagePool, PageRef, Pointer, PAGE_SIZE},
  utils::{SBox, SharedToken},
};

/**
 * Page reference annotated with its logical disk pointer.
 *
 * This is a thin wrapper used when code needs both the page bytes and the block
 * pointer that the cached page represents.
 */
pub struct RefedSlot {
  pointer: Pointer,
  page: PageRef<PAGE_SIZE>,
  dirty_blocks: NonNull<DirtyBlocks>,
  block_id: BlockId,
  modified: bool,
}
impl RefedSlot {
  const fn new(
    pointer: Pointer,
    page: PageRef<PAGE_SIZE>,
    dirty_blocks: &DirtyBlocks,
    block_id: BlockId,
  ) -> Self {
    Self {
      pointer,
      page,
      dirty_blocks: NonNull::from_ref(dirty_blocks),
      block_id,
      modified: false,
    }
  }
  pub const fn get_pointer(&self) -> Pointer {
    self.pointer
  }
  fn into_inner(self) -> PageRef<PAGE_SIZE> {
    self.page
  }

  const fn is_modified(&self) -> bool {
    self.modified
  }
}
impl AsRef<Page> for RefedSlot {
  fn as_ref(&self) -> &Page {
    &self.page
  }
}
impl AsMut<Page> for RefedSlot {
  fn as_mut(&mut self) -> &mut Page {
    if !self.modified {
      self.modified = true;
      unsafe { self.dirty_blocks.as_ref() }.insert(self.block_id);
    }
    &mut self.page
  }
}
unsafe impl Send for RefedSlot {}
unsafe impl Sync for RefedSlot {}

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
  page_pool: &'a PagePool<PAGE_SIZE>,
}
impl<'a> CachedSlot<'a> {
  pub fn new(
    block: &'a CachedBlock,
    dirty: &'a DirtyBlocks,
    block_id: BlockId,
    token: SharedToken<'a>,
    page_pool: &'a PagePool<PAGE_SIZE>,
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
    let mut shadow = self.page_pool.acquire();
    let latch = self.block.latch();
    shadow.copy_from(self.block.load_page().as_slice(), 0);
    let slot =
      RefedSlot::new(self.block.get_pointer(), shadow, self.dirty, self.block_id);

    WritableSlot {
      shadow: ManuallyDrop::new(slot),
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
pub struct WritableSlot<'a> {
  shadow: ManuallyDrop<RefedSlot>,
  latch: BlockLatch<'a>,
  _token: SharedToken<'a>,
}

impl<'a> Deref for WritableSlot<'a> {
  type Target = RefedSlot;

  fn deref(&self) -> &Self::Target {
    &self.shadow
  }
}
impl<'a> DerefMut for WritableSlot<'a> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.shadow
  }
}
impl<'a> Drop for WritableSlot<'a> {
  fn drop(&mut self) {
    let shadow = unsafe { ManuallyDrop::take(&mut self.shadow) };
    if shadow.is_modified() {
      self.latch.apply(shadow.into_inner());
    }
  }
}
