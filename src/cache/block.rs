use std::{
  cell::UnsafeCell,
  mem::MaybeUninit,
  sync::{Mutex, MutexGuard},
};

use crate::{
  disk::{Page, PageRef, PendingIO, Pointer, PAGE_SIZE},
  table::TableHandleRef,
  utils::{create_static_ref, AtomicRef, AtomicSBox, ShortenedMutex},
  Result,
};

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
    let static_ref = unsafe { create_static_ref::<Page>(&**page) };
    let handle = self.handle.disk().write_async(self.pointer, static_ref);
    PendingFlush {
      handle: Some(handle),
      _page: page,
    }
  }
}

pub struct PendingFlush {
  handle: Option<PendingIO>,
  _page: AtomicRef<PageRef<PAGE_SIZE>>,
}
impl PendingFlush {
  pub fn finalize(mut self) -> Result {
    self.handle.take().unwrap().wait_flatten()
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
 * installed. epoch is protected by batch mutation in writable slot.
 */
pub struct CachedBlock {
  metadata: UnsafeCell<MaybeUninit<BlockMetadata>>,
  latch: Mutex<u64>,
}
impl CachedBlock {
  #[inline]
  pub const fn uninit() -> Self {
    Self {
      metadata: UnsafeCell::new(MaybeUninit::uninit()),
      latch: Mutex::new(0),
    }
  }

  #[inline]
  pub fn latch(&self) -> BlockLatch<'_> {
    BlockLatch {
      pages: unsafe { &self.metadata_ref().page },
      guard: self.latch.l(),
    }
  }

  #[inline]
  pub const fn get_pointer(&self) -> Pointer {
    unsafe { self.metadata_ref().pointer }
  }

  #[inline]
  pub fn load_page(&self) -> AtomicRef<PageRef<PAGE_SIZE>> {
    unsafe { self.metadata_ref().page.load() }
  }

  const unsafe fn metadata_ref(&self) -> &BlockMetadata {
    unsafe { (*self.metadata.get()).assume_init_ref() }
  }

  #[inline]
  pub const fn handle(&self) -> &TableHandleRef {
    unsafe { &self.metadata_ref().handle }
  }

  /**
   * Write the current page to disk.
   */
  pub const fn flusher(&self) -> BlockFlusher<'_> {
    BlockFlusher::new(
      unsafe { &self.metadata_ref().page },
      self.handle(),
      self.get_pointer(),
    )
  }

  pub unsafe fn drop_in_place(&self) {
    unsafe { (*self.metadata.get()).assume_init_drop() };
  }

  pub unsafe fn replace(
    &self,
    page: PageRef<PAGE_SIZE>,
    pointer: Pointer,
    table: TableHandleRef,
  ) {
    let _ = (*self.metadata.get()).assume_init_read();
    self.init(page, pointer, table);
  }

  pub unsafe fn init(
    &self,
    page: PageRef<PAGE_SIZE>,
    pointer: Pointer,
    table: TableHandleRef,
  ) {
    let metadata = BlockMetadata::new(page, pointer, table);
    (*self.metadata.get()).write(metadata);
  }
}

struct BlockMetadata {
  page: AtomicSBox<PageRef<PAGE_SIZE>>,
  pointer: Pointer,
  handle: TableHandleRef,
}
impl BlockMetadata {
  fn new(page: PageRef<PAGE_SIZE>, pointer: Pointer, handle: TableHandleRef) -> Self {
    Self {
      page: AtomicSBox::new(page),
      pointer,
      handle,
    }
  }
}

unsafe impl Send for CachedBlock {}
unsafe impl Sync for CachedBlock {}
