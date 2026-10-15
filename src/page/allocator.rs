use std::{
  alloc::Layout,
  mem::ManuallyDrop,
  ops::{Deref, DerefMut},
  sync::Arc,
};

use crossbeam::{queue::ArrayQueue, utils::Backoff};

use super::{HeapBuffer, Page, ALIGN};

struct GlobalQueue<const N: usize>(ManuallyDrop<ArrayQueue<HeapBuffer>>);
impl<const N: usize> GlobalQueue<N> {
  fn new(capacity: usize) -> Self {
    Self(ManuallyDrop::new(ArrayQueue::new(capacity)))
  }
  fn store(&self, buf: HeapBuffer) {
    if let Err(err) = self.0.push(buf) {
      unsafe { err.deallocate(PageAllocator::<N>::LAYOUT) };
    }
  }
  fn acquire(&self) -> Option<HeapBuffer> {
    self.0.pop()
  }
}
impl<const N: usize> Drop for GlobalQueue<N> {
  fn drop(&mut self) {
    for buf in unsafe { ManuallyDrop::take(&mut self.0) } {
      unsafe { buf.deallocate(PageAllocator::<N>::LAYOUT) };
    }
  }
}
pub struct PageRef<const N: usize> {
  page: Page<N>,
  global: Arc<GlobalQueue<N>>,
}
impl<const N: usize> PageRef<N> {
  const fn new(buf: HeapBuffer, global: Arc<GlobalQueue<N>>) -> Self {
    Self {
      page: Page::new(buf.as_raw()),
      global,
    }
  }
}
impl<const N: usize> Drop for PageRef<N> {
  fn drop(&mut self) {
    let buf = unsafe { HeapBuffer::from_raw(self.page.as_ptr()) };
    self.global.store(buf);
  }
}
impl<const N: usize> Deref for PageRef<N> {
  type Target = Page<N>;
  fn deref(&self) -> &Self::Target {
    &self.page
  }
}
impl<const N: usize> DerefMut for PageRef<N> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.page
  }
}

pub struct PageAllocator<const N: usize> {
  global: Arc<GlobalQueue<N>>,
}
impl<const N: usize> PageAllocator<N> {
  const LAYOUT: Layout = {
    assert!(N.is_power_of_two());
    unsafe { Layout::from_size_align_unchecked(N, ALIGN) }
  };
  pub fn new(capacity: usize) -> Self {
    let global = Arc::new(GlobalQueue::new(capacity));
    for _ in 0..capacity {
      global.store(HeapBuffer::allocate(Self::LAYOUT));
    }
    Self { global }
  }

  pub fn allocate_new(&self) -> PageRef<N> {
    PageRef::new(HeapBuffer::allocate(Self::LAYOUT), self.global.clone())
  }

  pub fn allocate(&self) -> PageRef<N> {
    let backoff = Backoff::new();
    while !backoff.is_completed() {
      if let Some(buf) = self.global.acquire() {
        return PageRef::new(buf, self.global.clone());
      };
      backoff.snooze();
    }
    self.allocate_new()
  }
}
