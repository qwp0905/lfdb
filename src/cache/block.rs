use std::{
  cell::UnsafeCell,
  panic::RefUnwindSafe,
  sync::{Arc, Mutex, MutexGuard},
};

use crossbeam::utils::Backoff;

use crate::{
  disk::{PageRef, Pointer, PAGE_SIZE},
  table::TableHandle,
  thread::TaskHandle,
  utils::{ExclusivePin, ShortenedMutex, ToArc, UnsafeBorrow},
};

pub struct CachedBlock {
  page: UnsafeCell<Arc<PageRef<PAGE_SIZE>>>,
  page_pin: ExclusivePin,
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
      page: UnsafeCell::new(page.to_arc()),
      page_pin: ExclusivePin::new(),
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
    let backoff = Backoff::new();
    loop {
      if let Some(_token) = self.page_pin.try_shared() {
        return self.page.get().borrow_unsafe().clone();
      }
      backoff.snooze();
    }
  }
  pub fn store(&self, page: PageRef<PAGE_SIZE>) {
    let page = page.to_arc();
    let backoff = Backoff::new();
    loop {
      if let Some(_token) = self.page_pin.try_exclusive() {
        let _ = unsafe { self.page.get().replace(page) };
        return;
      }
      backoff.snooze();
    }
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
unsafe impl Send for CachedBlock {}
unsafe impl Sync for CachedBlock {}
impl RefUnwindSafe for CachedBlock {}
