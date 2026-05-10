use super::{Latch, LatchGuard};

use crate::{
  disk::{PageRef, Pointer, PAGE_SIZE},
  table::TableHandle,
  thread::TaskHandle,
  utils::{AtomicArc, SArc},
};

pub struct CachedBlock {
  page: AtomicArc<PageRef<PAGE_SIZE>>,
  pointer: Pointer,
  handle: SArc<TableHandle>,
  latch: Latch,
}
impl CachedBlock {
  #[inline]
  pub fn new(
    pointer: Pointer,
    page: PageRef<PAGE_SIZE>,
    handle: SArc<TableHandle>,
  ) -> Self {
    Self {
      page: AtomicArc::new(page),
      pointer,
      handle,
      latch: Latch::new(),
    }
  }

  #[inline]
  pub const fn get_pointer(&self) -> Pointer {
    self.pointer
  }

  #[inline]
  pub fn load_page(&self) -> SArc<PageRef<PAGE_SIZE>> {
    self.page.load()
  }
  pub fn store(&self, page: PageRef<PAGE_SIZE>) {
    self.page.store(page);
  }

  #[inline]
  pub fn latch(&self) -> LatchGuard<'_> {
    self.latch.lock_immediately()
  }
  #[inline]
  pub fn lazy_latch(&self) -> LatchGuard<'_> {
    self.latch.lock_lazily()
  }

  #[inline]
  pub fn flush_async(&self) -> TaskHandle<()> {
    self
      .handle
      .disk()
      .write_async(self.pointer, self.load_page())
  }

  #[inline]
  pub const fn handle(&self) -> &SArc<TableHandle> {
    &self.handle
  }
}
