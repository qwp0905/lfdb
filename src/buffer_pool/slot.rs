use std::{
  mem::{transmute, ManuallyDrop},
  sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use crossbeam::utils::Backoff;

use super::{Frame, FrameState, Shard, TempFrameState};
use crate::{
  disk::{DiskController, Page, PageRef, PAGE_SIZE},
  utils::{AtomicBitmap, ShortenedMutex, ShortenedRwLock},
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
    index: usize,
    disk: &'a DiskController<PAGE_SIZE>,
    shard: &'a Mutex<Shard>,
    is_peek: bool,
  ) -> Self {
    Self::Temp(TempSlot {
      state,
      index,
      disk,
      shard,
      is_peek,
    })
  }
  pub fn page(
    frame: &'a RwLock<Frame>,
    dirty: &'a AtomicBitmap,
    state: Arc<FrameState>,
  ) -> Self {
    Self::Page(PageSlot {
      frame,
      dirty,
      state,
    })
  }
  pub fn for_read<'b>(self) -> ReadableSlot<'b>
  where
    'a: 'b,
  {
    match self {
      Slot::Temp(temp) => ReadableSlot::Temp(temp.for_read()),
      Slot::Page(page) => ReadableSlot::Page(page.for_read()),
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
      WritableSlot::Temp(temp) => temp.guard.as_mut(),
      WritableSlot::Page(page) => page.guard.page_ref_mut().as_mut(),
    }
  }
}
impl<'a> AsRef<Page<PAGE_SIZE>> for WritableSlot<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    match self {
      WritableSlot::Temp(temp) => temp.guard.as_ref(),
      WritableSlot::Page(page) => page.guard.page_ref().as_ref(),
    }
  }
}
impl<'a> WritableSlot<'a> {
  pub fn get_index(&self) -> usize {
    match self {
      WritableSlot::Temp(temp) => temp.index,
      WritableSlot::Page(page) => page.guard.get_index(),
    }
  }
}

pub enum ReadableSlot<'a> {
  Temp(TempSlotRead<'a>),
  Page(PageSlotRead<'a>),
}
impl<'a> AsRef<Page<PAGE_SIZE>> for ReadableSlot<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    match self {
      ReadableSlot::Temp(temp) => temp.guard.as_ref(),
      ReadableSlot::Page(page) => page.guard.page_ref().as_ref(),
    }
  }
}

pub struct PageSlot<'a> {
  frame: &'a RwLock<Frame>,
  dirty: &'a AtomicBitmap,
  state: Arc<FrameState>,
}
impl<'a> PageSlot<'a> {
  fn for_read<'b>(self) -> PageSlotRead<'b>
  where
    'a: 'b,
  {
    PageSlotRead {
      guard: self.frame.rl(),
      state: self.state,
    }
  }

  fn for_write<'b>(self) -> PageSlotWrite<'b>
  where
    'a: 'b,
  {
    PageSlotWrite {
      guard: self.frame.wl(),
      dirty: self.dirty,
      state: self.state,
    }
  }
}
pub struct PageSlotWrite<'a> {
  guard: RwLockWriteGuard<'a, Frame>,
  dirty: &'a AtomicBitmap,
  state: Arc<FrameState>,
}

impl<'a> Drop for PageSlotWrite<'a> {
  fn drop(&mut self) {
    // Obtaining a WritableSlot is treated as equivalent to modifying the page.
    // We cannot know whether the caller actually modified it without expensive
    // byte-level comparison. The cost of an occasional unnecessary flush is
    // far lower than tracking write intent.
    self.dirty.insert(self.state.get_frame_id());
    self.state.unpin();
  }
}

pub struct PageSlotRead<'a> {
  guard: RwLockReadGuard<'a, Frame>,
  state: Arc<FrameState>,
}
impl<'a> Drop for PageSlotRead<'a> {
  fn drop(&mut self) {
    self.state.unpin();
  }
}

pub struct TempSlot<'a> {
  state: Arc<TempFrameState>,
  index: usize,
  disk: &'a DiskController<PAGE_SIZE>,
  shard: &'a Mutex<Shard>,
  is_peek: bool,
}
impl<'a> TempSlot<'a> {
  fn for_read<'b>(self) -> TempSlotRead<'b>
  where
    'a: 'b,
  {
    TempSlotRead {
      guard: ManuallyDrop::new(unsafe { transmute(self.state.for_read()) }),
      state: self.state.clone(),
      index: self.index,
      disk: self.disk,
      shard: self.shard,
      is_peek: self.is_peek,
    }
  }

  fn for_write<'b>(self) -> TempSlotWrite<'b>
  where
    'a: 'b,
  {
    TempSlotWrite {
      guard: ManuallyDrop::new(unsafe { transmute(self.state.for_write()) }),
      state: self.state.clone(),
      index: self.index,
      disk: self.disk,
      shard: self.shard,
      is_peek: self.is_peek,
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
  state: Arc<TempFrameState>,
  guard: ManuallyDrop<RwLockWriteGuard<'a, PageRef<PAGE_SIZE>>>,
  index: usize,
  disk: &'a DiskController<PAGE_SIZE>,
  shard: &'a Mutex<Shard>,
  is_peek: bool,
}

impl<'a> TempSlotWrite<'a> {
  fn release(&mut self) {
    unsafe { ManuallyDrop::drop(&mut self.guard) };
    let backoff = Backoff::new();
    while !self.state.try_evict() {
      backoff.snooze();
    }
    let _ = self.disk.write(self.index, &self.state.for_read());
    self.shard.l().remove_temp(self.index);
  }
}
impl<'a> Drop for TempSlotWrite<'a> {
  fn drop(&mut self) {
    // is_peek identifies the creator of this temp page, who is responsible
    // for cleanup (disk write + remove_temp). Non-creators simply unpin and exit.
    if self.is_peek {
      return self.release();
    }
    self.state.mark_dirty();
    self.state.unpin();
    unsafe { ManuallyDrop::drop(&mut self.guard) };
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
  guard: ManuallyDrop<RwLockReadGuard<'a, PageRef<PAGE_SIZE>>>,
  index: usize,
  disk: &'a DiskController<PAGE_SIZE>,
  shard: &'a Mutex<Shard>,
  is_peek: bool,
}
impl<'a> TempSlotRead<'a> {
  fn release(&mut self) {
    unsafe { ManuallyDrop::drop(&mut self.guard) };
    let backoff = Backoff::new();
    while !self.state.try_evict() {
      backoff.snooze();
    }
    if self.state.is_dirty() {
      let _ = self.disk.write(self.index, &self.state.for_read());
    }
    self.shard.l().remove_temp(self.index);
  }
}
impl<'a> Drop for TempSlotRead<'a> {
  fn drop(&mut self) {
    // is_peek identifies the creator of this temp page, who is responsible
    // for cleanup (disk write + remove_temp). Non-creators simply unpin and exit.
    if self.is_peek {
      return self.release();
    }

    self.state.unpin();
    unsafe { ManuallyDrop::drop(&mut self.guard) };
  }
}
