use std::{mem::ManuallyDrop, sync::Arc};

use crossbeam::queue::ArrayQueue;

use super::Page;
use crate::utils::ToArc;

/**
 * A handle to a pooled Page. The page is always present — ManuallyDrop
 * defers deallocation to Drop, where it is returned to the pool or freed.
 */
pub struct PageRef<const N: usize> {
  page: ManuallyDrop<Page<N>>,
  store: Arc<PageStore<N>>,
}
impl<const N: usize> PageRef<N> {
  fn from_exists(store: Arc<PageStore<N>>, page: Page<N>) -> Self {
    Self {
      page: ManuallyDrop::new(page),
      store,
    }
  }

  fn new(store: Arc<PageStore<N>>) -> Self {
    Self::from_exists(store, Page::new())
  }
}
impl<const N: usize> AsRef<Page<N>> for PageRef<N> {
  #[inline]
  fn as_ref(&self) -> &Page<N> {
    &self.page
  }
}
impl<const N: usize> AsMut<Page<N>> for PageRef<N> {
  #[inline]
  fn as_mut(&mut self) -> &mut Page<N> {
    &mut self.page
  }
}
impl<const N: usize> Drop for PageRef<N> {
  fn drop(&mut self) {
    let _ = self
      .store
      .data
      .push(unsafe { ManuallyDrop::take(&mut self.page) });
  }
}

/**
 * A pool of reusable Page buffers.
 * acquire() returns a pooled page if available, or allocates a new one.
 * Dropped pages are returned to the pool; excess pages beyond capacity are freed.
 */
pub struct PagePool<const N: usize> {
  store: Arc<PageStore<N>>,
}
impl<const N: usize> PagePool<N> {
  pub fn new(cap: usize) -> Self {
    Self {
      store: PageStore::new(cap).to_arc(),
    }
  }

  pub fn acquire(&self) -> PageRef<N> {
    if let Some(page) = self.store.data.pop() {
      return PageRef::from_exists(self.store.clone(), page);
    }
    PageRef::new(self.store.clone())
  }

  #[allow(unused)]
  pub fn len(&self) -> usize {
    self.store.data.len()
  }
}

struct PageStore<const N: usize> {
  data: ArrayQueue<Page<N>>,
}
impl<const N: usize> PageStore<N> {
  fn new(cap: usize) -> Self {
    Self {
      data: ArrayQueue::new(cap),
    }
  }
}
impl<const N: usize> Drop for PageStore<N> {
  fn drop(&mut self) {
    while let Some(page) = self.data.pop() {
      drop(page);
    }
  }
}

#[cfg(test)]
#[path = "tests/page_pool.rs"]
mod tests;
