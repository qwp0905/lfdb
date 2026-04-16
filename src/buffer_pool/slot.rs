use std::{
  mem::{transmute, ManuallyDrop},
  sync::{Arc, Mutex, MutexGuard},
};

use crossbeam::{
  epoch::{pin, Guard},
  utils::Backoff,
};

use super::{Frame, FrameState, Shard, TempFrameState};
use crate::{
  disk::{Page, PagePool, PageRef, Pointer, PAGE_SIZE},
  table::TableHandle,
  utils::{AtomicBitmap, ShortenedMutex, UnsafeBorrow},
};

/**
 * A handle to a buffer pool page, abstracting over LRU frames and temp pages.
 *
 * Callers only need to call for_read() or for_write() — the distinction between
 * Page and Temp is an internal detail. Dirty tracking and disk writes are handled
 * by the buffer pool itself when the slot is dropped.
 */
pub enum Slot<'a> {
  Temp(TempSlot<'a>),
  Page(PageSlot<'a>),
}
impl<'a> Slot<'a> {
  pub fn temp(
    state: Arc<TempFrameState>,
    pointer: Pointer,
    handle: Option<Arc<TableHandle>>,
    shard: &'a Mutex<Shard>,
    page_pool: &'a PagePool<PAGE_SIZE>,
  ) -> Self {
    Self::Temp(TempSlot {
      state,
      pointer,
      handle,
      shard,
      page_pool,
    })
  }
  pub fn page(
    frame: &'a Frame,
    dirty: &'a AtomicBitmap,
    state: Arc<FrameState>,
    page_pool: &'a PagePool<PAGE_SIZE>,
  ) -> Self {
    Self::Page(PageSlot {
      frame,
      dirty,
      state,
      page_pool,
    })
  }
  pub fn for_read<'b>(self) -> ReadonlySlot<'b>
  where
    'a: 'b,
  {
    match self {
      Slot::Temp(temp) => ReadonlySlot::Temp(temp.for_read()),
      Slot::Page(page) => ReadonlySlot::Page(page.for_read()),
    }
  }

  pub fn for_write<'b>(self) -> WritableSlot<'b>
  where
    'a: 'b,
  {
    match self {
      Slot::Temp(temp) => WritableSlot::Temp(temp.for_write()),
      Slot::Page(page) => WritableSlot::Page(page.for_write()),
    }
  }
}
pub enum WritableSlot<'a> {
  Temp(TempSlotWrite<'a>),
  Page(PageSlotWrite<'a>),
}
impl<'a> AsMut<Page<PAGE_SIZE>> for WritableSlot<'a> {
  fn as_mut(&mut self) -> &mut Page<PAGE_SIZE> {
    match self {
      WritableSlot::Temp(temp) => temp.shadow.as_mut(),
      WritableSlot::Page(page) => page.shadow.as_mut(),
    }
  }
}
impl<'a> AsRef<Page<PAGE_SIZE>> for WritableSlot<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    match self {
      WritableSlot::Temp(temp) => temp.shadow.as_ref(),
      WritableSlot::Page(page) => page.shadow.as_ref(),
    }
  }
}
impl<'a> WritableSlot<'a> {
  pub fn get_pointer(&self) -> Pointer {
    match self {
      WritableSlot::Temp(temp) => temp.pointer,
      WritableSlot::Page(page) => page.frame.get_pointer(),
    }
  }
}

pub enum ReadonlySlot<'a> {
  Temp(TempSlotRead<'a>),
  Page(PageSlotRead),
}
impl<'a> AsRef<Page<PAGE_SIZE>> for ReadonlySlot<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    match self {
      ReadonlySlot::Temp(temp) => temp.page.borrow_unsafe().as_ref(),
      ReadonlySlot::Page(page) => page.page.borrow_unsafe().as_ref(),
    }
  }
}

pub struct PageSlot<'a> {
  frame: &'a Frame,
  dirty: &'a AtomicBitmap,
  state: Arc<FrameState>,
  page_pool: &'a PagePool<PAGE_SIZE>,
}
impl<'a> PageSlot<'a> {
  fn for_read(self) -> PageSlotRead {
    let guard = pin();
    let page = self.frame.load_page(&guard);
    PageSlotRead {
      _guard: guard,
      page,
      state: self.state,
    }
  }

  fn for_write<'b>(self) -> PageSlotWrite<'b>
  where
    'a: 'b,
  {
    let guard = pin();
    let mut shadow = self.page_pool.acquire();
    let _latch = self.frame.latch();
    shadow
      .as_mut()
      .copy_from(self.frame.load_page(&guard).borrow_unsafe().as_ref());
    PageSlotWrite {
      shadow: ManuallyDrop::new(shadow),
      frame: self.frame,
      dirty: self.dirty,
      state: self.state,
      _latch,
    }
  }
}
pub struct PageSlotWrite<'a> {
  shadow: ManuallyDrop<PageRef<PAGE_SIZE>>,
  frame: &'a Frame,
  dirty: &'a AtomicBitmap,
  state: Arc<FrameState>,
  _latch: MutexGuard<'a, ()>,
}

impl<'a> Drop for PageSlotWrite<'a> {
  fn drop(&mut self) {
    // Obtaining a WritableSlot is treated as equivalent to modifying the page.
    // We cannot know whether the caller actually modified it without expensive
    // byte-level comparison. The cost of an occasional unnecessary flush is
    // far lower than tracking write intent.
    self.dirty.insert(self.state.get_frame_id());
    self
      .frame
      .store(unsafe { ManuallyDrop::take(&mut self.shadow) });
    self.state.unpin();
  }
}

pub struct PageSlotRead {
  page: *const PageRef<PAGE_SIZE>,
  state: Arc<FrameState>,
  _guard: Guard,
}
impl Drop for PageSlotRead {
  fn drop(&mut self) {
    self.state.unpin();
  }
}

pub struct TempSlot<'a> {
  state: Arc<TempFrameState>,
  pointer: Pointer,
  handle: Option<Arc<TableHandle>>,
  shard: &'a Mutex<Shard>,
  page_pool: &'a PagePool<PAGE_SIZE>,
}
impl<'a> TempSlot<'a> {
  fn for_read<'b>(self) -> TempSlotRead<'b>
  where
    'a: 'b,
  {
    let guard = pin();
    let page = self.state.load_page(&guard);
    TempSlotRead {
      state: self.state,
      page,
      pointer: self.pointer,
      handle: self.handle,
      shard: self.shard,
      guard,
    }
  }

  fn for_write<'b>(self) -> TempSlotWrite<'b>
  where
    'a: 'b,
  {
    let guard = pin();
    let mut shadow = self.page_pool.acquire();
    let latch = self.state.latch();
    shadow
      .as_mut()
      .copy_from(self.state.load_page(&guard).borrow_unsafe().as_ref());

    TempSlotWrite {
      shadow: ManuallyDrop::new(shadow),
      state: self.state.clone(),
      pointer: self.pointer,
      handle: self.handle,
      shard: self.shard,
      latch: ManuallyDrop::new(unsafe { transmute(latch) }),
    }
  }
}

/**
 * The guard is wrapped in ManuallyDrop to control drop order inside Drop::drop.
 * The lock must be released before try_evict() so that other threads waiting
 * on this page can proceed — Rust does not allow moving fields out of self in Drop,
 * so ManuallyDrop::drop is used to release the lock at the right moment.
 *
 * transmute is used to extend the guard's lifetime to match the struct,
 * since the borrow checker cannot infer that the guard outlives self here.
 */
pub struct TempSlotWrite<'a> {
  shadow: ManuallyDrop<PageRef<PAGE_SIZE>>,
  state: Arc<TempFrameState>,
  pointer: Pointer,
  handle: Option<Arc<TableHandle>>,
  shard: &'a Mutex<Shard>,
  latch: ManuallyDrop<MutexGuard<'a, ()>>,
}

impl<'a> TempSlotWrite<'a> {
  fn release(&mut self, handle: Arc<TableHandle>) {
    unsafe { ManuallyDrop::drop(&mut self.latch) };
    let backoff = Backoff::new();
    while !self.state.try_evict() {
      backoff.snooze();
    }
    let guard = pin();
    let page = self.state.load_page(&guard);
    let _ = handle.disk().write(self.pointer, page.borrow_unsafe());
    self
      .shard
      .l()
      .remove_temp(handle.metadata().get_id(), self.pointer);
  }
}
impl<'a> Drop for TempSlotWrite<'a> {
  fn drop(&mut self) {
    self
      .state
      .store(unsafe { ManuallyDrop::take(&mut self.shadow) });
    // handle identifies the creator of this temp page, who is responsible
    // for cleanup (disk write + remove_temp). Non-creators simply unpin and exit.
    if let Some(handle) = self.handle.take() {
      return self.release(handle);
    }
    self.state.mark_dirty();
    self.state.unpin();
    unsafe { ManuallyDrop::drop(&mut self.latch) };
  }
}

/**
 * The guard is wrapped in ManuallyDrop to control drop order inside Drop::drop.
 * The lock must be released before try_evict() so that other threads waiting
 * on this page can proceed — Rust does not allow moving fields out of self in Drop,
 * so ManuallyDrop::drop is used to release the lock at the right moment.
 *
 * transmute is used to extend the guard's lifetime to match the struct,
 * since the borrow checker cannot infer that the guard outlives self here.
 */
pub struct TempSlotRead<'a> {
  state: Arc<TempFrameState>,
  page: *const PageRef<PAGE_SIZE>,
  pointer: Pointer,
  handle: Option<Arc<TableHandle>>,
  shard: &'a Mutex<Shard>,
  guard: Guard,
}
impl<'a> TempSlotRead<'a> {
  fn release(&mut self, handle: Arc<TableHandle>) {
    let backoff = Backoff::new();
    while !self.state.try_evict() {
      backoff.snooze();
    }
    if self.state.is_dirty() {
      let page = self.state.load_page(&self.guard);
      let _ = handle.disk().write(self.pointer, page.borrow_unsafe());
    }
    self
      .shard
      .l()
      .remove_temp(handle.metadata().get_id(), self.pointer);
  }
}
impl<'a> Drop for TempSlotRead<'a> {
  fn drop(&mut self) {
    // handle identifies the creator of this temp page, who is responsible
    // for cleanup (disk write + remove_temp). Non-creators simply unpin and exit.
    if let Some(handle) = self.handle.take() {
      return self.release(handle);
    }

    self.state.unpin();
  }
}
