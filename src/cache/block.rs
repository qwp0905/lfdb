use std::{
  mem::transmute,
  sync::{Mutex, MutexGuard},
};

use crate::{
  disk::{AsyncIO, Page, PageRef, Pointer, PAGE_SIZE},
  table::TableHandleRef,
  utils::{AtomicSBox, SBox, ShortenedMutex},
  Result,
};

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
      guard: self.latch.l(),
    }
  }

  #[inline]
  pub const fn handle(&self) -> &TableHandleRef {
    &self.handle
  }

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
      guard: self.latch.l(),
    }
  }
}
