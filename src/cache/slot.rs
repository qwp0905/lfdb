use std::{mem::ManuallyDrop, sync::Arc};

use super::{BlockId, CachedBlock, LatchGuard};
use crate::{
  disk::{Page, PagePool, PageRef, Pointer, PAGE_SIZE},
  utils::{AtomicBitmap, SharedToken},
};

/**
 * A handle to a block cache page, abstracting over cached blocks and temp pages.
 *
 * Callers only need to call for_read() or for_write() — the distinction between
 * Page and Temp is an internal detail. Dirty tracking and disk writes are handled
 * by the block cache itself when the slot is dropped.
 */
pub struct CachedSlot<'a> {
  block: &'a CachedBlock,
  dirty: &'a AtomicBitmap,
  block_id: BlockId,
  token: SharedToken<'a>,
  page_pool: &'a PagePool<PAGE_SIZE>,
}
impl<'a> CachedSlot<'a> {
  pub fn new(
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
    let _latch = self.block.latch();
    shadow.copy_from(&**self.block.load_page());
    WritableSlot {
      shadow: ManuallyDrop::new(shadow),

      block: self.block,
      dirty: self.dirty,
      block_id: self.block_id,
      _token: self.token,
      _latch,
    }
  }
  pub fn for_lazy_write<'b>(self) -> WritableSlot<'b>
  where
    'a: 'b,
  {
    let mut shadow = self.page_pool.acquire();
    let _latch = self.block.lazy_latch();
    shadow.copy_from(&**self.block.load_page());
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

pub struct ReadonlySlot {
  page: Arc<PageRef<PAGE_SIZE>>,
}
impl ReadonlySlot {
  pub fn page(&self) -> Arc<PageRef<PAGE_SIZE>> {
    self.page.clone()
  }
}
impl AsRef<Page<PAGE_SIZE>> for ReadonlySlot {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    &*self.page
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
impl<'a> WritableSlot<'a> {
  pub fn get_pointer(&self) -> Pointer {
    self.block.get_pointer()
  }
}
impl<'a> AsRef<Page<PAGE_SIZE>> for WritableSlot<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    &*self.shadow
  }
}
impl<'a> AsMut<Page<PAGE_SIZE>> for WritableSlot<'a> {
  fn as_mut(&mut self) -> &mut Page<PAGE_SIZE> {
    &mut *self.shadow
  }
}
impl<'a> Drop for WritableSlot<'a> {
  fn drop(&mut self) {
    let shadow = unsafe { ManuallyDrop::take(&mut self.shadow) };
    self.dirty.insert(self.block_id);
    self.block.store(shadow);
  }
}
