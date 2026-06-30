use std::{
  mem::{transmute, ManuallyDrop},
  ops::{Deref, DerefMut},
};

use super::{BatchHandle, BatchHandler, BlockId, BlockLatch, CachedBlock};
use crate::{
  background::oneshot,
  disk::{Page, PagePool, PageRef, Pointer, PAGE_SIZE},
  utils::{AtomicBitmap, SBox, SharedToken},
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
 * Access interface for one cached block.
 *
 * A `CachedSlot` is returned after the block cache has found and pinned a block.
 * The caller then chooses the access mode: read the current page, write through
 * a shadow page, or join a batched mutation pass. The slot hides the cached page
 * replacement, dirty marking, and page-pool details behind those modes.
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
    shadow.copy_from(self.block.load_page().as_slice());
    WritableSlot {
      shadow: ManuallyDrop::new(RefedSlot::new(self.block.get_pointer(), shadow)),

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
 * same page snapshot without taking the block latch.
 */
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
    &self.page
  }
}

/**
 * Copy-on-write write guard for a cached block.
 *
 * The guard owns a shadow page copied from the current cached page. Callers
 * mutate that shadow page, and when the guard is dropped the shadow replaces the
 * cached page and advances the block epoch.
 */
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
  fn __mutate(self, handler: Box<BatchHandler<'static>>) {
    if !self.batch.register(handler) {
      return;
    }

    loop {
      let mut page = self.page_pool.acquire();
      {
        let mut latch = self.block.latch();
        self.dirty.insert(self.block_id);
        page.copy_from(self.block.load_page().as_slice());

        let mut slot = RefedSlot::new(self.block.get_pointer(), page);
        self.batch.flush_with(&mut slot);

        latch.apply(slot.into_inner());
      }

      if self.batch.try_release() {
        break;
      }
    }
  }
  pub fn mutate<T>(self, handler: impl FnOnce(&mut RefedSlot) -> T) -> T {
    let (o, f) = oneshot();
    let boxed: Box<BatchHandler> = Box::new(|slot| f.fulfill(handler(slot)));

    // SAFETY: `BatchHandle` stores handlers behind a `'static` type because a
    // different caller may become the batch owner and execute them. This function
    // waits for this handler's completion before returning, so any non-static
    // captures inside the closure cannot outlive the call.
    let handler =
      unsafe { transmute::<Box<BatchHandler>, Box<BatchHandler<'static>>>(boxed) };
    self.__mutate(handler);
    o.wait().unwrap()
  }
}
