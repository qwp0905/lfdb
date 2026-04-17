use std::{
  marker::PhantomData,
  mem::transmute,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, MutexGuard,
  },
};

use crossbeam::epoch::{pin, Atomic, Guard, Owned, Shared};

use crate::{
  disk::{PageRef, PAGE_SIZE},
  utils::{ExclusivePin, ExclusiveToken, SharedToken, ShortenedMutex},
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
  pin: ExclusivePin,
  page: Atomic<PageRef<PAGE_SIZE>>,
  dirty: AtomicBool,
  latch: Mutex<()>,
}

impl TempFrameState {
  pub fn new() -> Self {
    Self {
      pin: ExclusivePin::new(),
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
  pub fn try_evict(&self) -> Option<ExclusiveToken<'_>> {
    self.pin.try_exclusive()
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
  pub fn try_pin(&self) -> Option<SharedToken<'_>> {
    self.pin.try_shared()
  }

  #[inline]
  pub fn latch(&self) -> MutexGuard<'_, ()> {
    self.latch.l()
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

pub struct TempStateRef<'a, T> {
  state: Arc<TempFrameState>,
  token: T,
  _marker: PhantomData<&'a ()>,
}
impl<'a, T> TempStateRef<'a, T> {
  #[inline]
  pub fn state(&self) -> &TempFrameState {
    &self.state
  }
}

/**
 * transmute is allowed since Arc<TempFrameState> is valid until this struct.
 */
impl<'a> TempStateRef<'a, SharedToken<'a>> {
  #[inline]
  pub fn shared(state: &Arc<TempFrameState>) -> Option<Self> {
    let token = state.try_pin()?;
    Some(Self {
      state: state.clone(),
      token: unsafe { transmute(token) },
      _marker: PhantomData,
    })
  }

  #[inline]
  pub fn upgrade(self) -> TempStateRef<'a, ExclusiveToken<'a>> {
    TempStateRef {
      state: self.state,
      token: self.token.upgrade(),
      _marker: self._marker,
    }
  }
}

impl<'a> TempStateRef<'a, ExclusiveToken<'a>> {
  #[inline]
  pub fn exclusive(state: &Arc<TempFrameState>) -> Self {
    let token = state.try_evict().unwrap();
    Self {
      state: state.clone(),
      token: unsafe { transmute(token) },
      _marker: PhantomData,
    }
  }

  #[inline]
  pub fn downgrade(self) -> TempStateRef<'a, SharedToken<'a>> {
    TempStateRef {
      state: self.state,
      token: self.token.downgrade(),
      _marker: self._marker,
    }
  }
}
