use std::{mem::ManuallyDrop, sync::Arc};

use super::{BlockId, CachedBlock, LatchGuard};
use crate::{
  disk::{Page, PagePool, PageRef, Pointer, PAGE_SIZE},
  utils::{AtomicBitmap, SharedToken},
};

/**
 * A handle to a block cache page, abstracting over LRU blocks and temp pages.
 *
 * Callers only need to call for_read() or for_write() — the distinction between
 * Page and Temp is an internal detail. Dirty tracking and disk writes are handled
 * by the block cache itself when the slot is dropped.
 */
pub struct CacheSlot<'a> {
  block: &'a CachedBlock,
  dirty: &'a AtomicBitmap,
  block_id: BlockId,
  token: SharedToken<'a>,
  page_pool: &'a PagePool<PAGE_SIZE>,
}
impl<'a> CacheSlot<'a> {
  #[inline]
  pub const fn new(
    block: &'a CachedBlock,
    dirty: &'a AtomicBitmap,
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
  #[inline]
  pub fn for_read<'b>(self) -> ReadonlySlot<'b>
  where
    'a: 'b,
  {
    let page = self.block.load_page();
    ReadonlySlot {
      page,
      _token: self.token,
    }
  }

  #[inline]
  pub fn for_write<'b>(self) -> WritableSlot<'b>
  where
    'a: 'b,
  {
    let mut shadow = self.page_pool.acquire();
    let _latch = self.block.latch();
    shadow.copy_from(self.block.load_page().as_ref().as_ref());
    WritableSlot {
      shadow: ManuallyDrop::new(shadow),
      block: self.block,
      dirty: self.dirty,
      block_id: self.block_id,
      _token: self.token,
      _latch,
    }
  }
  #[inline]
  pub fn for_lazy_write<'b>(self) -> WritableSlot<'b>
  where
    'a: 'b,
  {
    let mut shadow = self.page_pool.acquire();
    let _latch = self.block.lazy_latch();
    shadow.copy_from(self.block.load_page().as_ref().as_ref());
    WritableSlot {
      shadow: ManuallyDrop::new(shadow),
      block: self.block,
      dirty: self.dirty,
      block_id: self.block_id,
      _token: self.token,
      _latch,
    }
  }
}
pub struct WritableSlot<'a> {
  shadow: ManuallyDrop<PageRef<PAGE_SIZE>>,
  block: &'a CachedBlock,
  dirty: &'a AtomicBitmap,
  block_id: BlockId,
  _token: SharedToken<'a>,
  _latch: LatchGuard<'a>,
}
impl<'a> AsMut<Page<PAGE_SIZE>> for WritableSlot<'a> {
  #[inline]
  fn as_mut(&mut self) -> &mut Page<PAGE_SIZE> {
    &mut self.shadow
  }
}
impl<'a> AsRef<Page<PAGE_SIZE>> for WritableSlot<'a> {
  #[inline]
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    &self.shadow
  }
}
impl<'a> WritableSlot<'a> {
  #[inline]
  pub const fn get_pointer(&self) -> Pointer {
    self.block.get_pointer()
  }
}

pub struct ReadonlySlot<'a> {
  page: Arc<PageRef<PAGE_SIZE>>,
  _token: SharedToken<'a>,
}
impl<'a> AsRef<Page<PAGE_SIZE>> for ReadonlySlot<'a> {
  #[inline]
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    &self.page
  }
}

impl<'a> Drop for WritableSlot<'a> {
  fn drop(&mut self) {
    // Obtaining a WritableSlot is treated as equivalent to modifying the page.
    // We cannot know whether the caller actually modified it without expensive
    // byte-level comparison. The cost of an occasional unnecessary flush is
    // far lower than tracking write intent.
    self.dirty.insert(self.block_id);
    self
      .block
      .store(unsafe { ManuallyDrop::take(&mut self.shadow) });
  }
}
