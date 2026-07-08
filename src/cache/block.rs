use std::mem::transmute;

use crate::{
  disk::{AsyncIO, Page, PageRef, Pointer, PAGE_SIZE},
  table::TableHandleRef,
  utils::{AtomicSBox, SBox},
  Result,
};

use parking_lot::{Mutex, MutexGuard};

/**
 * Exclusive update guard for a cached block.
 *
 * Applying a page installs the new page pointer and advances the block epoch.
 */
pub struct BlockLatch<'a> {
  pages: &'a AtomicSBox<PageRef<PAGE_SIZE>>,
  guard: MutexGuard<'a, u64>,
}
impl<'a> BlockLatch<'a> {
  pub fn apply(&mut self, page: PageRef<PAGE_SIZE>) {
    self.pages.store(page);
    *self.guard += 1;
  }
  pub fn epoch(&self) -> u64 {
    *self.guard
  }
}

pub struct BlockFlusher<'a> {
  pages: &'a AtomicSBox<PageRef<PAGE_SIZE>>,
  handle: &'a TableHandleRef,
  pointer: Pointer,
  guard: MutexGuard<'a, u64>,
}
impl<'a> BlockFlusher<'a> {
  pub fn submit(self) -> BlockFlushResult {
    let page = self.pages.load();
    let epoch = *self.guard;

    // SAFETY: `write_async` needs a `'static` page because the IO worker may run
    // after this function returns. `BlockFlushResult` keeps an `SBox` clone of the
    // loaded page, and `finalize(self)` waits for the async write before that clone
    // is dropped. Therefore the submitted page remains alive until the worker is
    // done with the slice.
    let static_ref = unsafe { transmute::<&'_ Page, &'static Page>(&**page) };
    let handle = self.handle.disk().write_async(self.pointer, static_ref);
    BlockFlushResult {
      epoch,
      handle,
      _page: page,
    }
  }
}
pub struct BlockFlushResult {
  epoch: u64,
  handle: AsyncIO,
  _page: SBox<PageRef<PAGE_SIZE>>,
}
impl BlockFlushResult {
  pub const fn epoch(&self) -> u64 {
    self.epoch
  }
  pub fn finalize(self) -> (u64, Result) {
    (self.epoch, self.handle.wait())
  }
}

/**
 * Cached page for one table block.
 *
 * The page pointer can be atomically swapped when a new page version is
 * installed. `latch` protects the block epoch: every installed page advances the
 * epoch, and a flush records the epoch it submitted so the caller can later tell
 * whether more changes happened after that flush started.
 */
pub struct CachedBlock {
  page: AtomicSBox<PageRef<PAGE_SIZE>>,
  pointer: Pointer,
  handle: TableHandleRef,
  latch: Mutex<u64>,
}
impl CachedBlock {
  #[inline]
  pub fn new(pointer: Pointer, page: PageRef<PAGE_SIZE>, handle: TableHandleRef) -> Self {
    Self {
      page: AtomicSBox::new(page),
      pointer,
      handle,
      latch: Mutex::new(0),
    }
  }

  #[inline]
  pub const fn get_pointer(&self) -> Pointer {
    self.pointer
  }

  #[inline]
  pub fn load_page(&self) -> SBox<PageRef<PAGE_SIZE>> {
    self.page.load()
  }

  #[inline]
  pub fn latch(&self) -> BlockLatch<'_> {
    BlockLatch {
      pages: &self.page,
      guard: self.latch.lock(),
    }
  }

  #[inline]
  pub const fn handle(&self) -> &TableHandleRef {
    &self.handle
  }

  /**
   * Write the current page without taking the block latch.
   *
   * Normal shared-access flushing must use `BlockFlusher` so the page snapshot and
   * epoch are recorded consistently. `flush_hard` is for callers that already
   * provide stronger external exclusion and therefore know the block cannot be
   * concurrently replaced while the write is issued.
   */
  pub fn flush_hard(&self) -> Result {
    let page = self.load_page();
    self.handle.disk().write(self.pointer, &**page)
  }

  /**
   * Create BlockFlusher which contains block latch.
   */
  pub fn flusher(&self) -> BlockFlusher<'_> {
    BlockFlusher {
      pages: &self.page,
      handle: &self.handle,
      pointer: self.pointer,
      guard: self.latch.lock(),
    }
  }
}
