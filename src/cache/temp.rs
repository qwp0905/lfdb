use std::{
  marker::PhantomData,
  mem::transmute,
  ops::Deref,
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
 * TempBlockState lifecycle:
 *
 * 1. Allocated by peek() (used by GC) — not promoted into the LRU table.
 * 2. If the page is not already in memory, it is loaded from disk into
 *    the temp page buffer.
 * 3. The caller reads or writes the page, behaving like a regular cached block.
 * 4. Unlike regular blocks, the owner of a temp page is responsible for
 *    writing it back to disk and releasing it. It is never handed off to
 *    the LRU eviction path.
 */
pub struct TempBlockState {
  pin: ExclusivePin,
  page: Atomic<PageRef<PAGE_SIZE>>,
  dirty: AtomicBool,
  latch: Mutex<()>,
}

impl TempBlockState {
  pub const fn new() -> Self {
    Self {
      pin: ExclusivePin::new(),
      page: Atomic::null(),
      dirty: AtomicBool::new(false),
      latch: Mutex::new(()),
    }
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
impl Drop for TempBlockState {
  fn drop(&mut self) {
    let g = pin();
    let old = self.page.swap(Shared::null(), Ordering::Release, &g);
    if old.is_null() {
      return;
    }
    unsafe { g.defer_destroy(old) };
  }
}

pub struct TempBlockRef<'a, T> {
  state: Arc<TempBlockState>,
  token: T,
  _marker: PhantomData<&'a ()>,
}
impl<'a, T> Deref for TempBlockRef<'a, T> {
  type Target = TempBlockState;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.state
  }
}

/**
 * transmute is allowed since Arc<TempBlockState> is valid until this struct.
 */
impl<'a> TempBlockRef<'a, SharedToken<'a>> {
  #[inline]
  pub fn shared(state: &Arc<TempBlockState>) -> Option<Self> {
    let token = state.try_pin()?;
    Some(Self {
      state: state.clone(),
      token: unsafe { transmute(token) },
      _marker: PhantomData,
    })
  }

  #[inline]
  pub fn upgrade(self) -> TempBlockRef<'a, ExclusiveToken<'a>> {
    TempBlockRef {
      state: self.state,
      token: self.token.upgrade(),
      _marker: self._marker,
    }
  }
}

impl<'a> TempBlockRef<'a, ExclusiveToken<'a>> {
  #[inline]
  pub fn exclusive(state: &Arc<TempBlockState>) -> Self {
    let token = state.pin.try_exclusive().unwrap();
    Self {
      state: state.clone(),
      token: unsafe { transmute(token) },
      _marker: PhantomData,
    }
  }

  pub fn get_state(&self) -> Arc<TempBlockState> {
    self.state.clone()
  }

  #[inline]
  pub fn downgrade(self) -> TempBlockRef<'a, SharedToken<'a>> {
    TempBlockRef {
      state: self.state,
      token: self.token.downgrade(),
      _marker: self._marker,
    }
  }
}
