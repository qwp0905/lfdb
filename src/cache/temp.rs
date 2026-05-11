use std::{
  cell::{Cell, OnceCell},
  mem::transmute,
  ops::Deref,
  sync::Arc,
};

use crate::{
  cache::latch::{Latch, LatchGuard},
  disk::{PageRef, Pointer, PAGE_SIZE},
  table::TableHandle,
  thread::TaskHandle,
  utils::{AtomicArc, ExclusivePin, ExclusiveToken, SharedToken},
};

pub struct TempBlock {
  page: OnceCell<AtomicArc<PageRef<PAGE_SIZE>>>,
  pointer: Pointer,
  pin: ExclusivePin,
  dirty: Cell<bool>,
  table: OnceCell<Arc<TableHandle>>,
  latch: Latch,
}
impl TempBlock {
  pub fn new(pointer: Pointer) -> Self {
    Self {
      page: OnceCell::new(),
      pointer,
      pin: ExclusivePin::new(),
      dirty: Cell::new(false),
      table: OnceCell::new(),
      latch: Latch::new(),
    }
  }

  pub const fn get_pointer(&self) -> Pointer {
    self.pointer
  }

  #[inline]
  pub fn load_page(&self) -> Arc<PageRef<PAGE_SIZE>> {
    self.page.get().unwrap().load()
  }

  #[inline]
  pub fn store(&self, page: PageRef<PAGE_SIZE>) {
    self.page.get().unwrap().store(page);
  }

  pub fn init(&self, page: PageRef<PAGE_SIZE>, table: Arc<TableHandle>) {
    let _ = self.page.set(AtomicArc::new(page));
    let _ = self.table.set(table);
  }

  #[inline]
  pub fn mark_dirty(&self, flag: bool) {
    self.dirty.set(flag);
  }
  #[inline]
  pub fn is_dirty(&self) -> bool {
    self.dirty.get()
  }

  #[inline]
  pub fn try_pin(&self) -> Option<SharedToken<'_>> {
    self.pin.try_shared()
  }

  #[inline]
  pub fn latch(&self) -> LatchGuard<'_> {
    self.latch.lock_immediately()
  }
  #[inline]
  pub fn lazy_latch(&self) -> LatchGuard<'_> {
    self.latch.lock_lazily()
  }

  pub fn flush_async(&self) -> TaskHandle<()> {
    self
      .table
      .get()
      .unwrap()
      .disk()
      .write_async(self.pointer, self.load_page())
  }
  pub fn table(&self) -> &Arc<TableHandle> {
    self.table.get().unwrap()
  }
}
unsafe impl Send for TempBlock {}
unsafe impl Sync for TempBlock {}

pub struct TempBlockRef<T> {
  block: Arc<TempBlock>,
  token: T,
}
impl<'a, T> Deref for TempBlockRef<T> {
  type Target = TempBlock;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &*self.block
  }
}

/**
 * transmute is allowed since Arc<TempBlockState> is valid until this struct.
 */
impl<'a> TempBlockRef<SharedToken<'a>> {
  #[inline]
  pub fn shared(block: &Arc<TempBlock>) -> Option<Self> {
    let token = block.try_pin()?;
    Some(Self {
      block: block.clone(),
      token: unsafe { transmute(token) },
    })
  }

  pub fn into_inner(self) -> Arc<TempBlock> {
    self.block
  }

  #[inline]
  pub fn upgrade(self) -> TempBlockRef<ExclusiveToken<'a>> {
    TempBlockRef {
      block: self.block,
      token: self.token.upgrade(),
    }
  }
}

impl<'a> TempBlockRef<ExclusiveToken<'a>> {
  #[inline]
  pub fn exclusive(block: &Arc<TempBlock>) -> Option<Self> {
    let token = block.pin.try_exclusive()?;
    Some(Self {
      block: block.clone(),
      token: unsafe { transmute(token) },
    })
  }

  pub fn get_block(&self) -> Arc<TempBlock> {
    self.block.clone()
  }

  #[inline]
  pub fn downgrade(self) -> TempBlockRef<SharedToken<'a>> {
    TempBlockRef {
      block: self.block,
      token: self.token.downgrade(),
    }
  }
}
