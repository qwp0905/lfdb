use std::{pin::pin, ptr::NonNull};

use super::{BatchGroup, BatchHandle, BlockId, CachedBlock, DirtyBlocks};
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
  batch_handle: &'a BatchHandle<RefedSlot>,
  block_id: BlockId,
  token: Option<SharedToken<'a>>,
  page_pool: &'a PagePool<PAGE_SIZE>,
  batch_group: &'a BatchGroup,
}
impl<'a> CachedSlot<'a> {
  pub fn new(
    block: &'a CachedBlock,
    dirty: &'a DirtyBlocks,
    batch_handle: &'a BatchHandle<RefedSlot>,
    block_id: BlockId,
    token: Option<SharedToken<'a>>,
    page_pool: &'a PagePool<PAGE_SIZE>,
    batch_group: &'a BatchGroup,
  ) -> Self {
    Self {
      block,
      dirty,
      batch_handle,
      block_id,
      token,
      page_pool,
      batch_group,
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
    WritableSlot {
      block: self.block,
      batch: self.batch_handle,
      page_pool: self.page_pool,
      dirty: self.dirty,
      block_id: self.block_id,
      group: self.batch_group,
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
  block: &'a CachedBlock,
  batch: &'a BatchHandle<RefedSlot>,
  page_pool: &'a PagePool<PAGE_SIZE>,
  dirty: &'a DirtyBlocks,
  group: &'a BatchGroup,
  block_id: BlockId,
  _token: Option<SharedToken<'a>>,
}
impl<'a> WritableSlot<'a> {
  pub fn mutate<T, F>(self, handler: F) -> T
  where
    T: Send,
    F: FnOnce(&mut RefedSlot) -> T + Unpin + Send,
  {
    let mut pinned = pin!(self.group.create_fn(|slot| handler(slot)));
    if !self.batch.register(pinned.as_mut().task()) {
      return pinned.wait();
    }

    loop {
      let mut page = self.page_pool.acquire();
      page.copy_from(self.block.load_page().as_slice(), 0);

      let mut slot =
        RefedSlot::new(self.block.get_pointer(), page, self.dirty, self.block_id);
      for mut task in self.batch.drain_tasks() {
        // SAFETY: Since `BatchFn` is pinned and its address does not change,
        // it can be accessed safely.
        unsafe { task.call_with(&mut slot) };
        if slot.modified {
          let mut replacement = self.page_pool.acquire();
          replacement.copy_from(slot.as_ref().as_slice(), 0);
          unsafe { self.block.advance_epoch(replacement) };
          slot.modified = false;
        }
        unsafe { task.complete() };
      }

      if self.batch.try_release() {
        break;
      }
    }

    pinned.wait()
  }
}
