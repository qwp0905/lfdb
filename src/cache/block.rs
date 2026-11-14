use std::{
  cell::UnsafeCell,
  mem::{ManuallyDrop, MaybeUninit},
  sync::{atomic::Ordering, Mutex, MutexGuard, RwLock},
};

use crate::{
  disk::{PendingIO, Pointer},
  page::{Page, PageRef, PAGE_SIZE},
  table::TableHandleRef,
  utils::{create_static_ref, SBox, ShortenedMutex, ShortenedRwLock},
  wal::{AtomicWriteSeq, WriteSeq},
  Result,
};

/**
 * Exclusive update guard for a cached block.
 *
 * The page and the write seq that made it dirty are installed together under this
 * guard, so both read under the same latch always match.
 */
pub struct BlockLatch<'a> {
  metadata: &'a BlockMetadata,
  dirty: &'a AtomicWriteSeq,
  _guard: MutexGuard<'a, ()>,
}
impl<'a> BlockLatch<'a> {
  pub fn apply(&mut self, page: PageRef<PAGE_SIZE>, write_seq: WriteSeq) {
    self.metadata.store_page(page);
    self.dirty.store(write_seq, Ordering::Relaxed);
  }
}

struct PendingFlushInner {
  pending: PendingIO,
  _page: SBox<PageRef<PAGE_SIZE>>,
}
impl PendingFlushInner {
  const fn new(pending: PendingIO, _page: SBox<PageRef<PAGE_SIZE>>) -> Self {
    Self { pending, _page }
  }
}

pub struct PendingFlush(ManuallyDrop<PendingFlushInner>);
impl PendingFlush {
  const fn new(pending: PendingIO, page: SBox<PageRef<PAGE_SIZE>>) -> Self {
    Self(ManuallyDrop::new(PendingFlushInner::new(pending, page)))
  }
  pub fn finalize(self) -> Result {
    let mut this = ManuallyDrop::new(self);
    let inner = unsafe { ManuallyDrop::take(&mut this.0) };
    inner.pending.wait_flatten()
  }
}
impl Drop for PendingFlush {
  fn drop(&mut self) {
    let inner = unsafe { ManuallyDrop::take(&mut self.0) };
    let _ = inner.pending.wait();
  }
}

const CLEAN: WriteSeq = WriteSeq::MAX;

pub struct DirtySnapshot<'a> {
  snapshot: ManuallyDrop<SBox<PageRef<PAGE_SIZE>>>,
  block: &'a CachedBlock,
  write_seq: WriteSeq,
}
impl<'a> DirtySnapshot<'a> {
  const fn new(
    snapshot: SBox<PageRef<PAGE_SIZE>>,
    block: &'a CachedBlock,
    write_seq: WriteSeq,
  ) -> Self {
    Self {
      snapshot: ManuallyDrop::new(snapshot),
      block,
      write_seq,
    }
  }

  pub const fn get_write_seq(&self) -> WriteSeq {
    self.write_seq
  }

  pub fn submit(self) -> PendingDirty<'a> {
    let mut this = ManuallyDrop::new(self);
    let page = unsafe { ManuallyDrop::take(&mut this.snapshot) };
    let pending = ManuallyDrop::new(this.block.submit_page(page));
    PendingDirty {
      pending,
      block: this.block,
      write_seq: this.write_seq,
    }
  }
}
impl<'a> Drop for DirtySnapshot<'a> {
  fn drop(&mut self) {
    unsafe { ManuallyDrop::drop(&mut self.snapshot) };
    self.block.rollback_dirty(self.write_seq);
  }
}

pub struct PendingDirty<'a> {
  pending: ManuallyDrop<PendingFlush>,
  block: &'a CachedBlock,
  write_seq: WriteSeq,
}
impl<'a> PendingDirty<'a> {
  pub fn finalize(self) -> Result {
    let mut this = ManuallyDrop::new(self);
    let pending = unsafe { ManuallyDrop::take(&mut this.pending) };
    let Err(err) = pending.finalize() else {
      return Ok(());
    };
    this.block.rollback_dirty(this.write_seq);
    Err(err)
  }
}
impl<'a> Drop for PendingDirty<'a> {
  fn drop(&mut self) {
    unsafe { ManuallyDrop::drop(&mut self.pending) };
    self.block.rollback_dirty(self.write_seq);
  }
}

/**
 * Cached page for one table block.
 *
 * The page pointer can be atomically swapped when a new page version is
 * installed. `dirty` holds the write seq of the newest change not yet on disk. It
 * is written only under the latch and read without it.
 */
pub struct CachedBlock {
  metadata: UnsafeCell<MaybeUninit<BlockMetadata>>,
  dirty: AtomicWriteSeq,
  latch: Mutex<()>,
}
impl CachedBlock {
  #[inline]
  pub const fn uninit() -> Self {
    Self {
      metadata: UnsafeCell::new(MaybeUninit::uninit()),
      dirty: AtomicWriteSeq::new(CLEAN),
      latch: Mutex::new(()),
    }
  }

  #[inline]
  pub fn latch(&self) -> BlockLatch<'_> {
    BlockLatch {
      metadata: unsafe { self.metadata_ref() },
      dirty: &self.dirty,
      _guard: self.latch.l(),
    }
  }

  pub fn get_dirty(&self) -> Option<WriteSeq> {
    let seq = self.dirty.load(Ordering::Relaxed);
    (seq != CLEAN).then_some(seq)
  }

  pub fn clear_dirty(&self) {
    self.dirty.store(CLEAN, Ordering::Relaxed);
  }

  fn rollback_dirty(&self, write_seq: WriteSeq) {
    let _latch = self.latch();
    if self.get_dirty().is_none() {
      self.dirty.store(write_seq, Ordering::Relaxed);
    }
  }

  #[inline]
  pub const fn get_pointer(&self) -> Pointer {
    unsafe { self.metadata_ref().pointer }
  }

  #[inline]
  pub fn load_page(&self) -> SBox<PageRef<PAGE_SIZE>> {
    unsafe { self.metadata_ref().load_page() }
  }

  const unsafe fn metadata_ref(&self) -> &BlockMetadata {
    unsafe { (*self.metadata.get()).assume_init_ref() }
  }

  #[inline]
  pub const fn handle(&self) -> &TableHandleRef {
    unsafe { &self.metadata_ref().handle }
  }

  fn submit_page(&self, page: SBox<PageRef<PAGE_SIZE>>) -> PendingFlush {
    let metadata = unsafe { self.metadata_ref() };
    // SAFETY: `write_async` needs a `'static` page because the IO worker may run
    // after this function returns. `PendingFlush` keeps an `SBox` clone of the
    // loaded page, and `finalize(self)` waits for the async write before that clone
    // is dropped. Therefore the submitted page remains alive until the worker is
    // done with the slice.
    let static_ref = unsafe { create_static_ref::<Page>(&**page) };
    let pending = metadata
      .handle
      .disk()
      .write_async(metadata.pointer, static_ref);
    PendingFlush::new(pending, page)
  }

  pub fn flush_dirty(&self, boundary: WriteSeq) -> Option<DirtySnapshot<'_>> {
    let _latch = self.latch();
    let dirty = self.get_dirty()?;
    if dirty >= boundary {
      return None;
    }
    self.clear_dirty();
    let page = self.load_page();
    Some(DirtySnapshot::new(page, self, dirty))
  }

  /**
   * Write the current page to disk.
   */
  pub fn submit_flush(&self) -> PendingFlush {
    let page = self.load_page();
    self.submit_page(page)
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
  page: RwLock<SBox<PageRef<PAGE_SIZE>>>,
  pointer: Pointer,
  handle: TableHandleRef,
}
impl BlockMetadata {
  fn new(page: PageRef<PAGE_SIZE>, pointer: Pointer, handle: TableHandleRef) -> Self {
    Self {
      page: RwLock::new(SBox::new(page)),
      pointer,
      handle,
    }
  }
  fn load_page(&self) -> SBox<PageRef<PAGE_SIZE>> {
    self.page.rl().clone()
  }
  fn store_page(&self, page: PageRef<PAGE_SIZE>) {
    let page = SBox::new(page);
    *self.page.wl() = page;
  }
}

unsafe impl Send for CachedBlock {}
unsafe impl Sync for CachedBlock {}
