use std::sync::{Arc, Mutex, MutexGuard};

use crate::{
  disk::{PageRef, Pointer, PAGE_SIZE},
  table::TableHandle,
  thread::TaskHandle,
  utils::{AtomicArc, ShortenedMutex},
};

pub struct CachedBlock {
  page: AtomicArc<PageRef<PAGE_SIZE>>,
  pointer: Pointer,
  handle: Arc<TableHandle>,
  latch: Mutex<()>,
}
impl CachedBlock {
  #[inline]
  pub fn new(
    pointer: Pointer,
    page: PageRef<PAGE_SIZE>,
    handle: Arc<TableHandle>,
  ) -> Self {
    Self {
      page: AtomicArc::new(page),
      pointer,
      handle,
      latch: Mutex::new(()),
    }
  }

  #[inline]
  pub const fn get_pointer(&self) -> Pointer {
    self.pointer
  }

  #[inline]
  pub fn load_page(&self) -> Arc<PageRef<PAGE_SIZE>> {
    self.page.load()
  }
  pub fn store(&self, page: PageRef<PAGE_SIZE>) {
    self.page.store(page);
  }

  #[inline]
  pub fn latch(&self) -> MutexGuard<'_, ()> {
    self.latch.l()
  }

  #[inline]
  pub fn flush_async(&self) -> TaskHandle<()> {
    self
      .handle
      .disk()
      .write_async(self.pointer, self.load_page())
  }

  #[inline]
  pub const fn handle(&self) -> &Arc<TableHandle> {
    &self.handle
  }
}
