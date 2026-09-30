use std::{
  alloc::{alloc_zeroed, dealloc, handle_alloc_error, Layout},
  ops::{Deref, DerefMut},
  sync::Arc,
};

use crossbeam::{queue::ArrayQueue, utils::Backoff};

use super::Page;

pub const ALIGN: usize = 512;

enum Source<const N: usize> {
  Borrowed(Arc<BufStore<N>>),
  Owned,
}

/**
 * Owned handle to a pooled page.
 *
 * `PageRef` keeps the page in `ManuallyDrop` because dropping the handle should
 * normally return the page to the pool instead of freeing it. If the pool is
 * already full, the failed `push` drops the page normally and releases the
 * allocation.
 */
pub struct PageRef<const N: usize> {
  page: Page<N>,
  source: Source<N>,
}
impl<const N: usize> PageRef<N> {
  const LAYOUT: Layout = {
    assert!(N.is_power_of_two());
    unsafe { Layout::from_size_align_unchecked(N, ALIGN) }
  };
  const fn from_exists(page: Page<N>, store: Arc<BufStore<N>>) -> Self {
    Self {
      page,
      source: Source::Borrowed(store),
    }
  }

  fn new() -> Self {
    let ptr = unsafe { alloc_zeroed(Self::LAYOUT) };
    let page = Page::new(ptr);
    Self {
      page,
      source: Source::Owned,
    }
  }
  fn release(ptr: *mut u8) {
    unsafe { dealloc(ptr, Self::LAYOUT) };
  }
}
impl<const N: usize> Deref for PageRef<N> {
  type Target = Page<N>;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.page
  }
}
impl<const N: usize> DerefMut for PageRef<N> {
  #[inline]
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.page
  }
}
impl<const N: usize> Drop for PageRef<N> {
  fn drop(&mut self) {
    match &self.source {
      Source::Borrowed(store) => store.release(self.page.as_ptr()),
      Source::Owned => Self::release(self.page.as_ptr()),
    }
  }
}

struct BufStore<const N: usize> {
  bytes: *mut u8,
  released: ArrayQueue<usize>,
  layout: Layout,
}
impl<const N: usize> BufStore<N> {
  fn new(capacity: usize) -> Self {
    let released = ArrayQueue::new(capacity);
    let mut offset = 0;
    for _ in 0..capacity {
      let _ = released.push(offset);
      offset += N;
    }

    let layout = unsafe { Layout::from_size_align_unchecked(offset, ALIGN) };
    let ptr = unsafe { alloc_zeroed(layout) };
    if ptr.is_null() {
      handle_alloc_error(layout);
    }

    Self {
      bytes: ptr,
      released,
      layout,
    }
  }
  fn release(&self, ptr: *mut u8) {
    let offset = unsafe { ptr.byte_offset_from_unsigned(self.bytes) };
    let _ = self.released.push(offset);
  }

  fn pop(&self) -> Option<Page<N>> {
    let offset = self.released.pop()?;
    Some(Page::new(unsafe { self.bytes.add(offset) }))
  }
}
impl<const N: usize> Drop for BufStore<N> {
  fn drop(&mut self) {
    unsafe { dealloc(self.bytes, self.layout) };
  }
}
unsafe impl<const N: usize> Send for BufStore<N> {}
unsafe impl<const N: usize> Sync for BufStore<N> {}

/**
 * Bounded pool of reusable aligned pages.
 *
 * `PagePool` reduces heap allocation churn for direct-I/O pages. `acquire`
 * returns a recycled page when one is available and allocates a new page only
 * when the pool is empty. When the returned `PageRef` is dropped, the page is
 * returned to the pool if there is capacity.
 */
pub struct PagePool<const N: usize> {
  store: Arc<BufStore<N>>,
}
impl<const N: usize> PagePool<N> {
  pub fn new(cap: usize) -> Self {
    Self {
      store: Arc::new(BufStore::new(cap)),
    }
  }

  pub fn acquire(&self) -> PageRef<N> {
    let backoff = Backoff::new();
    while !backoff.is_completed() {
      if let Some(page) = self.store.pop() {
        return PageRef::from_exists(page, self.store.clone());
      }
      backoff.snooze();
    }
    PageRef::new()
  }

  #[cfg(test)]
  pub fn len(&self) -> usize {
    self.store.released.len()
  }
}

#[cfg(test)]
#[path = "tests/page_pool.rs"]
mod tests;
