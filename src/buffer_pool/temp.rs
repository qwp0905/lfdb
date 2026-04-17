use std::sync::{
  atomic::{AtomicBool, Ordering},
  Mutex, MutexGuard,
};

use crossbeam::epoch::{pin, Atomic, Guard, Owned, Shared};

use super::EvictionPin;

use crate::{
  disk::{PageRef, PAGE_SIZE},
  utils::ShortenedMutex,
};

/**
 * TempFrameState lifecycle:
 *
 * 1. Allocated by peek() (used by GC) — not promoted into the LRU table.
 * 2. If the page is not already in memory, it is loaded from disk into
 *    the temp page buffer.
 * 3. The caller reads or writes the page, behaving like a regular frame.
 * 4. Unlike regular frames, the owner of a temp page is responsible for
 *    writing it back to disk and releasing it. It is never handed off to
 *    the LRU eviction path.
 */
pub struct TempFrameState {
  pin: EvictionPin,
  page: Atomic<PageRef<PAGE_SIZE>>,
  dirty: AtomicBool,
  latch: Mutex<()>,
}

impl TempFrameState {
  pub fn new() -> Self {
    Self {
      pin: EvictionPin::evicting(),
      page: Atomic::null(),
      dirty: AtomicBool::new(false),
      latch: Default::default(),
    }
  }

  /**
   * Unlike FrameState which waits for pin == 0 (no readers),
   * TempFrameState waits for pin == 1 — the owner's own pin.
   * The owner is always responsible for cleanup, so eviction
   * is safe as soon as no other reader holds a pin.
   */
  pub fn try_evict(&self) -> bool {
    self.pin.try_evict_owned()
  }

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
  pub fn mark_dirty(&self) {
    self.dirty.fetch_or(true, Ordering::Release);
  }
  #[inline]
  pub fn is_dirty(&self) -> bool {
    self.dirty.load(Ordering::Acquire)
  }

  #[inline]
  pub fn try_pin(&self) -> bool {
    self.pin.try_pin()
  }

  pub fn latch(&self) -> MutexGuard<'_, ()> {
    self.latch.l()
  }

  /**
   * Releases the eviction lock and sets the pin count to the given value.
   * Safe to use a plain store here because the eviction flag guarantees
   * exclusive access — no other thread can pin or evict while it is set.
   */
  #[inline]
  pub fn completion_evict(&self) {
    self.pin.owned();
  }

  pub fn unpin(&self) {
    self.pin.unpin()
  }
}
impl Drop for TempFrameState {
  fn drop(&mut self) {
    let g = pin();
    let old = self.page.swap(Shared::null(), Ordering::Release, &g);
    if old.is_null() {
      return;
    }
    unsafe { g.defer_destroy(old) };
  }
}
