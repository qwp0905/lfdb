use std::{
  cell::OnceCell,
  marker::PhantomData,
  mem::transmute,
  ops::Deref,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, MutexGuard,
  },
};

use crate::{
  disk::{PageRef, PAGE_SIZE},
  utils::{AtomicArc, ExclusivePin, ExclusiveToken, SharedToken, ShortenedMutex},
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
  page: OnceCell<AtomicArc<PageRef<PAGE_SIZE>>>,
  block_pin: ExclusivePin,
  dirty: AtomicBool,
  latch: Mutex<()>,
}

impl TempBlockState {
  pub fn new() -> Self {
    Self {
      page: OnceCell::new(),
      block_pin: ExclusivePin::new(),
      dirty: AtomicBool::new(false),
      latch: Mutex::new(()),
    }
  }

  #[inline]
  pub fn load_page(&self) -> Arc<PageRef<PAGE_SIZE>> {
    self.page.get().unwrap().load()
  }

  #[inline]
  pub fn store(&self, page: PageRef<PAGE_SIZE>) {
    self.page.get().unwrap().store(page);
  }

  pub fn init(&self, page: PageRef<PAGE_SIZE>) {
    let _ = self.page.set(AtomicArc::new(page));
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
    self.block_pin.try_shared()
  }

  #[inline]
  pub fn latch(&self) -> MutexGuard<'_, ()> {
    self.latch.l()
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
  pub fn exclusive(state: &Arc<TempBlockState>) -> Option<Self> {
    let token = state.block_pin.try_exclusive()?;
    Some(Self {
      state: state.clone(),
      token: unsafe { transmute(token) },
      _marker: PhantomData,
    })
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
