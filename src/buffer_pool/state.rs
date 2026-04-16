use std::sync::{
  atomic::{AtomicBool, AtomicU32, Ordering},
  Mutex, MutexGuard,
};

use crossbeam::{
  epoch::{pin, Atomic, Guard, Owned, Shared},
  utils::Backoff,
};

use crate::{
  disk::{PageRef, PAGE_SIZE},
  utils::ShortenedMutex,
};

/**
 * The MSB (bit 31) is the eviction flag; the remaining 31 bits are the
 * pin count. Both are packed into a single atomic so that eviction and
 * pinning can be toggled with a single CAS — analogous to an RwLock
 * where eviction is the write lock and each pin is a read lock.
 */
const EVICTION_BIT: u32 = 1 << 31;

/**
 * A per-frame access token that tracks concurrent access to a buffer pool frame.
 *
 * Acts as a lock: readers increment the pin count while accessing the frame,
 * and eviction requires exclusive access (EVICTION_BIT set, pin == 0).
 * Unlike TempFrameState, FrameState does not own the page data itself —
 * it only controls access to the frame.
 */
pub struct FrameState {
  pin: AtomicU32,
  frame_id: usize,
}
impl FrameState {
  pub fn new(frame_id: usize) -> Self {
    Self {
      pin: AtomicU32::new(EVICTION_BIT),
      frame_id,
    }
  }
  pub fn try_evict(&self) -> bool {
    self
      .pin
      .compare_exchange(0, EVICTION_BIT, Ordering::Acquire, Ordering::Relaxed)
      .is_ok()
  }
  pub fn try_pin(&self) -> bool {
    let backoff = Backoff::new();
    loop {
      let current = self.pin.load(Ordering::Acquire);
      if current & EVICTION_BIT != 0 {
        return false;
      }

      if self
        .pin
        .compare_exchange(current, current + 1, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
      {
        return true;
      }
      backoff.spin();
    }
  }

  /**
   * Releases the eviction lock and sets the pin count to the given value.
   * Safe to use a plain store here because the eviction flag guarantees
   * exclusive access — no other thread can pin or evict while it is set.
   */
  pub fn completion_evict(&self, pin: u32) {
    self.pin.store(pin, Ordering::Release);
  }

  pub fn get_frame_id(&self) -> usize {
    self.frame_id
  }
  pub fn unpin(&self) {
    self.pin.fetch_sub(1, Ordering::Release);
  }
}

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
  pin: AtomicU32,
  page: Atomic<PageRef<PAGE_SIZE>>,
  dirty: AtomicBool,
  latch: Mutex<()>,
}

impl TempFrameState {
  pub fn new() -> Self {
    Self {
      pin: AtomicU32::new(EVICTION_BIT),
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
    self
      .pin
      .compare_exchange(1, EVICTION_BIT, Ordering::Acquire, Ordering::Relaxed)
      .is_ok()
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

  pub fn mark_dirty(&self) {
    self.dirty.fetch_or(true, Ordering::Release);
  }
  pub fn is_dirty(&self) -> bool {
    self.dirty.load(Ordering::Acquire)
  }

  pub fn try_pin(&self) -> bool {
    let backoff = Backoff::new();
    loop {
      let current = self.pin.load(Ordering::Acquire);
      if current & EVICTION_BIT != 0 {
        return false;
      }

      if self
        .pin
        .compare_exchange(current, current + 1, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
      {
        return true;
      }
      backoff.spin();
    }
  }

  pub fn latch(&self) -> MutexGuard<'_, ()> {
    self.latch.l()
  }

  /**
   * Releases the eviction lock and sets the pin count to the given value.
   * Safe to use a plain store here because the eviction flag guarantees
   * exclusive access — no other thread can pin or evict while it is set.
   */
  pub fn completion_evict(&self, pin: u32) {
    self.pin.store(pin, Ordering::Release);
  }

  pub fn unpin(&self) {
    self.pin.fetch_sub(1, Ordering::Release);
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
