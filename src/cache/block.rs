use std::{
  mem::transmute,
  sync::{Arc, Mutex, MutexGuard},
};

use crate::{
  disk::{PageRef, Pointer, PAGE_SIZE},
  table::TableHandleRef,
  utils::{AtomicArc, ShortenedMutex},
  Result,
};

pub type LatchGuard<'a> = MutexGuard<'a, ()>;

pub struct CachedBlock {
  page: AtomicArc<PageRef<PAGE_SIZE>>,
  pointer: Pointer,
  handle: TableHandleRef,
  latch: Mutex<()>,
}
impl CachedBlock {
  #[inline]
  pub fn new(pointer: Pointer, page: PageRef<PAGE_SIZE>, handle: TableHandleRef) -> Self {
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
  pub fn latch(&self) -> LatchGuard<'_> {
    self.latch.l()
  }

  pub fn flush(&self) -> Result {
    let page = self.load_page();
    self
      .handle
      .disk()
      .write_async(self.pointer, unsafe { transmute(&**page) })
      .wait()
  }

  #[inline]
  pub const fn handle(&self) -> &TableHandleRef {
    &self.handle
  }
}
