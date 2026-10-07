use std::{
  ops::{Deref, DerefMut},
  sync::Arc,
};

use crossbeam::{queue::ArrayQueue, utils::Backoff};

use crate::background::ThreadParker;

use super::{AlignedBuf, Page};

pub struct PageRef<const N: usize> {
  page: Page<N>,
  global: Arc<GlobalQueue>,
}
impl<const N: usize> PageRef<N> {
  const fn new(ptr: *mut u8, global: Arc<GlobalQueue>) -> Self {
    Self {
      page: Page::new(ptr),
      global,
    }
  }
}
impl<const N: usize> Drop for PageRef<N> {
  fn drop(&mut self) {
    self.global.store(self.page.as_ptr())
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
  global: Arc<GlobalQueue>,
}
impl<const N: usize> PageAllocator<N> {
  pub fn new(capacity: usize) -> Self {
    Self {
      global: Arc::new(GlobalQueue::new(capacity, N)),
    }
  }

  fn create_with(&self, ptr: *mut u8) -> PageRef<N> {
    PageRef::new(ptr, self.global.clone())
  }

  pub fn allocate(&self) -> PageRef<N> {
    let ptr = self.global.pop();
    self.create_with(ptr)
  }

  pub fn try_allocate(&self) -> Option<PageRef<N>> {
    self.global.try_pop().map(|ptr| self.create_with(ptr))
  }
}

struct GlobalQueue {
  free: ArrayQueue<usize>,
  parker: ThreadParker,
  buf: AlignedBuf,
}
impl GlobalQueue {
  fn new(capacity: usize, page_size: usize) -> Self {
    let buf = AlignedBuf::new(capacity * page_size);
    let free = ArrayQueue::new(capacity);
    let mut offset = 0;
    while offset < buf.len() {
      let _ = free.push(offset);
      offset += page_size;
    }
    Self {
      free,
      buf,
      parker: ThreadParker::new(),
    }
  }

  fn store(&self, ptr: *mut u8) {
    let offset = unsafe { ptr.offset_from_unsigned(self.buf.as_ptr()) };
    let _ = self.free.push(offset);
    self.parker.wake_once();
  }

  fn try_pop(&self) -> Option<*mut u8> {
    self
      .free
      .pop()
      .map(|offset| unsafe { self.buf.as_ptr().add(offset) })
  }

  fn pop(&self) -> *mut u8 {
    let backoff = Backoff::new();
    loop {
      while !backoff.is_completed() {
        if let Some(offset) = self.free.pop() {
          return unsafe { self.buf.as_ptr().add(offset) };
        }
        backoff.snooze();
      }

      if let Some(offset) = self.parker.park_or_cancel(|| self.free.pop()) {
        return unsafe { self.buf.as_ptr().add(offset) };
      }
      backoff.reset();
    }
  }
}
unsafe impl Send for GlobalQueue {}
unsafe impl Sync for GlobalQueue {}
