use std::sync::{atomic::Ordering, Arc, Mutex, MutexGuard};

use crossbeam::epoch::{pin, Atomic, Guard, Owned, Shared};

use crate::{
  disk::{PageRef, Pointer, PAGE_SIZE},
  table::TableHandle,
  thread::TaskHandle,
  utils::{ShortenedMutex, UnsafeBorrow},
};

pub struct CachedBlock {
  page: Atomic<PageRef<PAGE_SIZE>>,
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
      page: Atomic::new(page),
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
  pub fn load_page<'a>(&self, guard: &'a Guard) -> *const PageRef<PAGE_SIZE> {
    self.page.load(Ordering::Acquire, guard).as_raw()
  }
  pub fn store(&self, page: PageRef<PAGE_SIZE>) {
    let g = pin();
    let old = self.page.swap(Owned::new(page), Ordering::Release, &g);
    if old.is_null() {
      return;
    }
    unsafe { g.defer_destroy(old) };
  }

  #[inline]
  pub fn latch(&self) -> MutexGuard<'_, ()> {
    self.latch.l()
  }

  /**
   * Guard must be live until async write is done.
   */
  #[inline]
  pub fn flush_async<'a>(&self, guard: &'a Guard) -> TaskHandle<()> {
    self
      .handle
      .disk()
      .write_async(self.pointer, self.load_page(guard).borrow_unsafe())
  }

  #[inline]
  pub const fn handle(&self) -> &Arc<TableHandle> {
    &self.handle
  }
}

impl Drop for CachedBlock {
  fn drop(&mut self) {
    let g = pin();
    let old = self.page.swap(Shared::null(), Ordering::Release, &g);
    if old.is_null() {
      return;
    }
    unsafe { g.defer_destroy(old) };
  }
}
