use std::mem::transmute;

use crate::{
  disk::{Page, PageRef, PendingIO, Pointer, PAGE_SIZE},
  table::TableHandleRef,
  utils::{AtomicSBox, SBox},
  Result,
};

pub struct BlockFlusher<'a> {
  pages: &'a AtomicSBox<PageRef<PAGE_SIZE>>,
  handle: &'a TableHandleRef,
  pointer: Pointer,
}
impl<'a> BlockFlusher<'a> {
  const fn new(
    pages: &'a AtomicSBox<PageRef<PAGE_SIZE>>,
    handle: &'a TableHandleRef,
    pointer: Pointer,
  ) -> Self {
    Self {
      pages,
      handle,
      pointer,
    }
  }
  pub fn submit(self) -> PendingFlush {
    let page = self.pages.load();

    // SAFETY: `write_async` needs a `'static` page because the IO worker may run
    // after this function returns. `PendingFlush` keeps an `SBox` clone of the
    // loaded page, and `finalize(self)` waits for the async write before that clone
    // is dropped. Therefore the submitted page remains alive until the worker is
    // done with the slice.
    let static_ref = unsafe { transmute::<&'_ Page, &'static Page>(&**page) };
    let handle = self.handle.disk().write_async(self.pointer, static_ref);
    PendingFlush {
      handle: Some(handle),
      _page: page,
    }
  }
}

pub struct PendingFlush {
  handle: Option<PendingIO>,
  _page: SBox<PageRef<PAGE_SIZE>>,
}
impl PendingFlush {
  pub fn finalize(mut self) -> Result {
    self.handle.take().unwrap().wait()
  }
}
impl Drop for PendingFlush {
  fn drop(&mut self) {
    let Some(handle) = self.handle.take() else {
      return;
    };
    let _ = handle.wait();
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
}
impl CachedBlock {
  #[inline]
  pub fn new(pointer: Pointer, page: PageRef<PAGE_SIZE>, handle: TableHandleRef) -> Self {
    Self {
      page: AtomicSBox::new(page),
      pointer,
      handle,
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
  pub fn store_page(&self, page: PageRef<PAGE_SIZE>) {
    self.page.store(page);
  }

  #[inline]
  pub const fn handle(&self) -> &TableHandleRef {
    &self.handle
  }

  /**
   * Write the current page without taking the block latch.
   */
  pub const fn flusher(&self) -> BlockFlusher<'_> {
    BlockFlusher::new(&self.page, &self.handle, self.pointer)
  }
}
