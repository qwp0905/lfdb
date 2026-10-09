use std::{
  alloc::{alloc_zeroed, dealloc, Layout},
  mem::ManuallyDrop,
  ops::{Deref, DerefMut},
  sync::Arc,
};

use crossbeam::{queue::ArrayQueue, utils::Backoff};

use super::{Page, ALIGN};

struct OwnedPage<const N: usize>(Page<N>);
impl<const N: usize> OwnedPage<N> {
  const LAYOUT: Layout = {
    assert!(N.is_power_of_two());
    unsafe { Layout::from_size_align_unchecked(N, ALIGN) }
  };
  fn allocate() -> Self {
    Self(Page::new(unsafe { alloc_zeroed(Self::LAYOUT) }))
  }
}
impl<const N: usize> Drop for OwnedPage<N> {
  fn drop(&mut self) {
    unsafe { dealloc(self.0.as_ptr(), Self::LAYOUT) }
  }
}

type GlobalQueue<const N: usize> = ArrayQueue<OwnedPage<N>>;
pub struct PageRef<const N: usize> {
  page: ManuallyDrop<OwnedPage<N>>,
  global: Arc<GlobalQueue<N>>,
}
impl<const N: usize> PageRef<N> {
  const fn new(page: OwnedPage<N>, global: Arc<GlobalQueue<N>>) -> Self {
    Self {
      page: ManuallyDrop::new(page),
      global,
    }
  }
}
impl<const N: usize> Drop for PageRef<N> {
  fn drop(&mut self) {
    let page = unsafe { ManuallyDrop::take(&mut self.page) };
    if let Err(err) = self.global.push(page) {
      drop(err);
    }
  }
}
impl<const N: usize> Deref for PageRef<N> {
  type Target = Page<N>;
  fn deref(&self) -> &Self::Target {
    &self.page.0
  }
}
impl<const N: usize> DerefMut for PageRef<N> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.page.0
  }
}

pub struct PageAllocator<const N: usize> {
  global: Arc<GlobalQueue<N>>,
}
impl<const N: usize> PageAllocator<N> {
  pub fn new(capacity: usize) -> Self {
    let global = GlobalQueue::new(capacity);
    for _ in 0..capacity {
      let _ = global.push(OwnedPage::allocate());
    }
    Self {
      global: Arc::new(global),
    }
  }

  pub fn allocate(&self) -> PageRef<N> {
    let backoff = Backoff::new();
    while !backoff.is_completed() {
      if let Some(page) = self.global.pop() {
        return PageRef::new(page, self.global.clone());
      };
      backoff.snooze();
    }
    PageRef::new(OwnedPage::allocate(), self.global.clone())
  }
}
