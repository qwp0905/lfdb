use std::{
  mem::{transmute, ManuallyDrop},
  sync::Arc,
};

use super::{BlockId, CachedBlock, DirtyTables, LatchGuard, TempBlockRef, TempGuard};
use crate::{
  disk::{Page, PagePool, PageRef, Pointer, PAGE_SIZE},
  table::TableHandle,
  utils::{AtomicBitmap, SharedToken},
};

/**
 * A handle to a block cache page, abstracting over LRU blocks and temp pages.
 *
 * Callers only need to call for_read() or for_write() — the distinction between
 * Page and Temp is an internal detail. Dirty tracking and disk writes are handled
 * by the block cache itself when the slot is dropped.
 */
pub enum CacheSlot<'a> {
  Temp(TempSlot<'a>),
  Page(PageSlot<'a>),
}
impl<'a> CacheSlot<'a> {
  #[inline]
  pub const fn temp(
    state: TempBlockRef<'a, SharedToken<'a>>,
    pointer: Pointer,
    guard: Option<(TempGuard<'a>, Arc<TableHandle>, &'a DirtyTables)>,
    page_pool: &'a PagePool<PAGE_SIZE>,
  ) -> Self {
    Self::Temp(TempSlot {
      state,
      pointer,
      guard,
      page_pool,
    })
  }
  #[inline]
  pub const fn page(
    block: &'a CachedBlock,
    dirty: &'a AtomicBitmap,
    block_id: BlockId,
    token: SharedToken<'a>,
    page_pool: &'a PagePool<PAGE_SIZE>,
  ) -> Self {
    Self::Page(PageSlot {
      block,
      dirty,
      block_id,
      token,
      page_pool,
    })
  }
  #[inline]
  pub fn for_read<'b>(self) -> ReadonlySlot<'b>
  where
    'a: 'b,
  {
    match self {
      CacheSlot::Temp(temp) => ReadonlySlot::Temp(temp.for_read()),
      CacheSlot::Page(page) => ReadonlySlot::Page(page.for_read()),
    }
  }

  #[inline]
  pub fn for_write<'b>(self) -> WritableSlot<'b>
  where
    'a: 'b,
  {
    match self {
      CacheSlot::Temp(temp) => WritableSlot::Temp(temp.for_write()),
      CacheSlot::Page(page) => WritableSlot::Page(page.for_write()),
    }
  }
  #[inline]
  pub fn for_lazy_write<'b>(self) -> WritableSlot<'b>
  where
    'a: 'b,
  {
    match self {
      CacheSlot::Temp(temp) => WritableSlot::Temp(temp.for_lazy_write()),
      CacheSlot::Page(page) => WritableSlot::Page(page.for_lazy_write()),
    }
  }
}
pub enum WritableSlot<'a> {
  Temp(TempSlotWrite<'a>),
  Page(PageSlotWrite<'a>),
}
impl<'a> AsMut<Page<PAGE_SIZE>> for WritableSlot<'a> {
  #[inline]
  fn as_mut(&mut self) -> &mut Page<PAGE_SIZE> {
    match self {
      WritableSlot::Temp(temp) => &mut temp.shadow,
      WritableSlot::Page(page) => &mut page.shadow,
    }
  }
}
impl<'a> AsRef<Page<PAGE_SIZE>> for WritableSlot<'a> {
  #[inline]
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    match self {
      WritableSlot::Temp(temp) => &temp.shadow,
      WritableSlot::Page(page) => &page.shadow,
    }
  }
}
impl<'a> WritableSlot<'a> {
  #[inline]
  pub const fn get_pointer(&self) -> Pointer {
    match self {
      WritableSlot::Temp(temp) => temp.pointer,
      WritableSlot::Page(page) => page.block.get_pointer(),
    }
  }
}

pub enum ReadonlySlot<'a> {
  Temp(TempSlotRead<'a>),
  Page(PageSlotRead<'a>),
}
impl<'a> AsRef<Page<PAGE_SIZE>> for ReadonlySlot<'a> {
  #[inline]
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    match self {
      ReadonlySlot::Temp(temp) => &temp.page,
      ReadonlySlot::Page(page) => &page.page,
    }
  }
}

pub struct PageSlot<'a> {
  block: &'a CachedBlock,
  dirty: &'a AtomicBitmap,
  block_id: BlockId,
  token: SharedToken<'a>,
  page_pool: &'a PagePool<PAGE_SIZE>,
}
impl<'a> PageSlot<'a> {
  #[inline]
  fn for_read<'b>(self) -> PageSlotRead<'b>
  where
    'a: 'b,
  {
    let page = self.block.load_page();
    PageSlotRead {
      page,
      _token: self.token,
    }
  }

  fn for_write<'b>(self) -> PageSlotWrite<'b>
  where
    'a: 'b,
  {
    let mut shadow = self.page_pool.acquire();
    let _latch = self.block.latch();
    shadow.copy_from(self.block.load_page().as_ref().as_ref());
    PageSlotWrite {
      shadow: ManuallyDrop::new(shadow),
      block: self.block,
      dirty: self.dirty,
      block_id: self.block_id,
      _token: self.token,
      _latch,
    }
  }

  fn for_lazy_write<'b>(self) -> PageSlotWrite<'b>
  where
    'a: 'b,
  {
    let mut shadow = self.page_pool.acquire();
    let _latch = self.block.lazy_latch();
    shadow.copy_from(self.block.load_page().as_ref().as_ref());
    PageSlotWrite {
      shadow: ManuallyDrop::new(shadow),
      block: self.block,
      dirty: self.dirty,
      block_id: self.block_id,
      _token: self.token,
      _latch,
    }
  }
}
pub struct PageSlotWrite<'a> {
  shadow: ManuallyDrop<PageRef<PAGE_SIZE>>,
  block: &'a CachedBlock,
  dirty: &'a AtomicBitmap,
  block_id: BlockId,
  _token: SharedToken<'a>,
  _latch: LatchGuard<'a>,
}

impl<'a> Drop for PageSlotWrite<'a> {
  fn drop(&mut self) {
    // Obtaining a WritableSlot is treated as equivalent to modifying the page.
    // We cannot know whether the caller actually modified it without expensive
    // byte-level comparison. The cost of an occasional unnecessary flush is
    // far lower than tracking write intent.
    self.dirty.insert(self.block_id);
    self
      .block
      .store(unsafe { ManuallyDrop::take(&mut self.shadow) });
  }
}

pub struct PageSlotRead<'a> {
  page: Arc<PageRef<PAGE_SIZE>>,
  _token: SharedToken<'a>,
}

pub struct TempSlot<'a> {
  state: TempBlockRef<'a, SharedToken<'a>>,
  pointer: Pointer,
  guard: Option<(TempGuard<'a>, Arc<TableHandle>, &'a DirtyTables)>,
  page_pool: &'a PagePool<PAGE_SIZE>,
}
impl<'a> TempSlot<'a> {
  #[inline]
  fn for_read<'b>(self) -> TempSlotRead<'b>
  where
    'a: 'b,
  {
    let page = self.state.load_page();
    TempSlotRead {
      state: ManuallyDrop::new(self.state),
      page: ManuallyDrop::new(page),
      pointer: self.pointer,
      temp_guard: self.guard,
    }
  }

  fn for_write<'b>(self) -> TempSlotWrite<'b>
  where
    'a: 'b,
  {
    let mut shadow = self.page_pool.acquire();
    let latch = unsafe { transmute(self.state.latch()) };
    shadow.copy_from(self.state.load_page().as_ref().as_ref());

    TempSlotWrite {
      shadow: ManuallyDrop::new(shadow),
      state: ManuallyDrop::new(self.state),
      pointer: self.pointer,
      guard: self.guard,
      latch: ManuallyDrop::new(latch),
    }
  }
  fn for_lazy_write<'b>(self) -> TempSlotWrite<'b>
  where
    'a: 'b,
  {
    let mut shadow = self.page_pool.acquire();
    let latch = unsafe { transmute(self.state.lazy_latch()) };
    shadow.copy_from(self.state.load_page().as_ref().as_ref());

    TempSlotWrite {
      shadow: ManuallyDrop::new(shadow),
      state: ManuallyDrop::new(self.state),
      pointer: self.pointer,
      guard: self.guard,
      latch: ManuallyDrop::new(latch),
    }
  }
}

/**
 * The guard is wrapped in ManuallyDrop to control drop order inside Drop::drop.
 * The lock must be released before upgrade() so that other threads waiting
 * on this page can proceed — Rust does not allow moving fields out of self in Drop,
 * so ManuallyDrop::drop is used to release the lock and upgrade the state at the right moment.
 *
 * transmute is used to extend the guard's lifetime to match the struct,
 * since the borrow checker cannot infer that the guard outlives self here.
 */
pub struct TempSlotWrite<'a> {
  shadow: ManuallyDrop<PageRef<PAGE_SIZE>>,
  state: ManuallyDrop<TempBlockRef<'a, SharedToken<'a>>>,
  pointer: Pointer,
  guard: Option<(TempGuard<'a>, Arc<TableHandle>, &'a DirtyTables)>,
  latch: ManuallyDrop<LatchGuard<'a>>,
}

impl<'a> TempSlotWrite<'a> {
  fn release(
    &mut self,
    handle: Arc<TableHandle>,
    _guard: TempGuard<'a>,
    dirty: &'a DirtyTables,
  ) {
    unsafe { ManuallyDrop::drop(&mut self.latch) };
    let state = unsafe { ManuallyDrop::take(&mut self.state) }.upgrade();

    let _ = handle.disk().write(self.pointer, state.load_page());
    dirty.mark(&handle);
  }
}
impl<'a> Drop for TempSlotWrite<'a> {
  fn drop(&mut self) {
    self
      .state
      .store(unsafe { ManuallyDrop::take(&mut self.shadow) });

    // guard identifies the creator of this temp page, who is responsible
    // for cleanup (disk write + remove_temp). Non-creators simply unpin and exit.
    if let Some((guard, handle, dirty)) = self.guard.take() {
      return self.release(handle, guard, dirty);
    }
    self.state.mark_dirty();
    unsafe { ManuallyDrop::drop(&mut self.latch) };
    unsafe { ManuallyDrop::drop(&mut self.state) };
  }
}

/**
 * The guard is wrapped in ManuallyDrop to control drop order inside Drop::drop.
 * Rust does not allow moving fields out of self in Drop,
 * so ManuallyDrop::drop is used to upgrade the state at the right moment.
 *
 * transmute is used to extend the guard's lifetime to match the struct,
 * since the borrow checker cannot infer that the guard outlives self here.
 */
pub struct TempSlotRead<'a> {
  state: ManuallyDrop<TempBlockRef<'a, SharedToken<'a>>>,
  page: ManuallyDrop<Arc<PageRef<PAGE_SIZE>>>,
  pointer: Pointer,
  temp_guard: Option<(TempGuard<'a>, Arc<TableHandle>, &'a DirtyTables)>,
}
impl<'a> TempSlotRead<'a> {
  fn release(
    &mut self,
    handle: Arc<TableHandle>,
    _guard: TempGuard<'a>,
    dirty: &'a DirtyTables,
  ) {
    let state = unsafe { ManuallyDrop::take(&mut self.state) }.upgrade();
    if !state.is_dirty() {
      return;
    }

    let _ = handle.disk().write(self.pointer, state.load_page());
    dirty.mark(&handle);
  }
}
impl<'a> Drop for TempSlotRead<'a> {
  #[inline]
  fn drop(&mut self) {
    unsafe { ManuallyDrop::drop(&mut self.page) };
    // block_guard identifies the creator of this temp page, who is responsible
    // for cleanup (disk write + remove_temp). Non-creators simply unpin and exit.
    if let Some((guard, handle, dirty)) = self.temp_guard.take() {
      return self.release(handle, guard, dirty);
    }

    unsafe { ManuallyDrop::drop(&mut self.state) }
  }
}
