use std::sync::{Mutex, MutexGuard};

use crate::{
  disk::{PageRef, Pointer, PAGE_SIZE},
  table::TableHandleRef,
  utils::{AtomicSBox, SBox, ShortenedMutex},
  Result,
};

pub type LatchGuard<'a> = MutexGuard<'a, ()>;

pub struct CachedBlock {
  page: AtomicSBox<PageRef<PAGE_SIZE>>,
  pointer: Pointer,
  handle: TableHandleRef,
  latch: Mutex<()>,
}
impl CachedBlock {
  #[inline]
  pub fn new(pointer: Pointer, page: PageRef<PAGE_SIZE>, handle: TableHandleRef) -> Self {
    Self {
      page: AtomicSBox::new(page),
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
  pub fn load_page(&self) -> SBox<PageRef<PAGE_SIZE>> {
    self.page.load()
  }
  pub fn store(&self, page: PageRef<PAGE_SIZE>) {
    self.page.store(page);
  }

  #[inline]
  pub fn latch(&self) -> LatchGuard<'_> {
    self.latch.l()
  }

  pub fn flush(&self) -> Result {
    let page = self.load_page();
    self.handle.disk().write(self.pointer, &page)
  }

  #[inline]
  pub const fn handle(&self) -> &TableHandleRef {
    &self.handle
  }
}
