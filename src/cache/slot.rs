use std::{
  mem::{transmute, ManuallyDrop},
  ops::{Deref, DerefMut},
};

use super::{BatchHandle, BatchHandler, BlockId, BlockLatch, CachedBlock};
use crate::{
  disk::{Page, PagePool, PageRef, Pointer, PAGE_SIZE},
  utils::{AtomicBitmap, SBox, SharedToken},
  Result,
};

pub struct RefedSlot {
  pointer: Pointer,
  page: PageRef<PAGE_SIZE>,
}
impl RefedSlot {
  pub const fn new(pointer: Pointer, page: PageRef<PAGE_SIZE>) -> Self {
    Self { pointer, page }
  }
  pub const fn get_pointer(&self) -> Pointer {
    self.pointer
  }
  pub fn into_inner(self) -> PageRef<PAGE_SIZE> {
    self.page
  }
}
impl AsRef<Page> for RefedSlot {
  fn as_ref(&self) -> &Page {
    &self.page
  }
}
impl AsMut<Page> for RefedSlot {
  fn as_mut(&mut self) -> &mut Page {
    &mut self.page
  }
}

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
  batch_handle: &'a BatchHandle,
  block_id: BlockId,
  token: SharedToken<'a>,
  page_pool: &'a PagePool<PAGE_SIZE>,
}
impl<'a> CachedSlot<'a> {
  pub fn new(
    block: &'a CachedBlock,
    dirty: &'a AtomicBitmap,
    batch_handle: &'a BatchHandle,
    block_id: BlockId,
    token: SharedToken<'a>,
    page_pool: &'a PagePool<PAGE_SIZE>,
  ) -> Self {
    Self {
      block,
      dirty,
      batch_handle,
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
  pub fn for_batch<'b>(self) -> BatchSlot<'b>
  where
    'a: 'b,
  {
    BatchSlot {
      block: self.block,
      batch: self.batch_handle,
      page_pool: self.page_pool,
      dirty: self.dirty,
      block_id: self.block_id,
      _token: self.token,
    }
  }
  pub fn for_write<'b>(self) -> WritableSlot<'b>
  where
    'a: 'b,
  {
    let mut shadow = self.page_pool.acquire();
    let latch = self.block.latch();
    self.dirty.insert(self.block_id);
    shadow.copy_from(&**self.block.load_page());
    WritableSlot {
      shadow: ManuallyDrop::new(RefedSlot::new(self.block.get_pointer(), shadow)),

      latch,
      _token: self.token,
    }
  }
}

pub struct ReadonlySlot {
  page: SBox<PageRef<PAGE_SIZE>>,
}
impl ReadonlySlot {
  pub fn page(&self) -> SBox<PageRef<PAGE_SIZE>> {
    self.page.clone()
  }
}
impl AsRef<Page<PAGE_SIZE>> for ReadonlySlot {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    &*self.page
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
    self.latch.apply(shadow.into_inner());
  }
}

pub struct BatchSlot<'a> {
  block: &'a CachedBlock,
  batch: &'a BatchHandle,
  page_pool: &'a PagePool<PAGE_SIZE>,
  dirty: &'a AtomicBitmap,
  block_id: BlockId,
  _token: SharedToken<'a>,
}
impl<'a> BatchSlot<'a> {
  fn __mutate(self, handler: Box<BatchHandler<'_>>) -> Result {
    let (occupied, o) = self.batch.register(unsafe { transmute(handler) });
    if !occupied {
      return o.wait().flatten();
    }

    loop {
      let mut page = self.page_pool.acquire();
      {
        let mut latch = self.block.latch();
        self.dirty.insert(self.block_id);
        page.copy_from(&**self.block.load_page());

        let mut slot = RefedSlot::new(self.block.get_pointer(), page);
        self.batch.flush_with(&mut slot);

        latch.apply(slot.into_inner());
      }

      if self.batch.try_release() {
        break;
      }
    }

    o.wait().flatten()
  }
  pub fn mutate(self, handler: impl FnOnce(&mut RefedSlot) -> Result) -> Result {
    self.__mutate(Box::new(handler))
  }
}
