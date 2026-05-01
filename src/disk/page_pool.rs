use std::{
  mem::ManuallyDrop,
  ops::{Deref, DerefMut},
  sync::Arc,
};

use crossbeam::{queue::ArrayQueue, utils::Backoff};

use super::Page;
use crate::utils::ToArc;

/**
 * A handle to a pooled Page. The page is always present — ManuallyDrop
 * defers deallocation to Drop, where it is returned to the pool or freed.
 */
pub struct PageRef<const N: usize> {
  page: ManuallyDrop<Page<N>>,
  store: Arc<ArrayQueue<Page<N>>>,
}
impl<const N: usize> PageRef<N> {
  const fn from_exists(store: Arc<ArrayQueue<Page<N>>>, page: Page<N>) -> Self {
    Self {
      page: ManuallyDrop::new(page),
      store,
    }
  }

  fn new(store: Arc<ArrayQueue<Page<N>>>) -> Self {
    Self::from_exists(store, Page::new())
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
    let _ = self
      .store
      .push(unsafe { ManuallyDrop::take(&mut self.page) });
  }
}

/**
 * A pool of reusable Page buffers.
 * acquire() returns a pooled page if available, or allocates a new one.
 * Dropped pages are returned to the pool; excess pages beyond capacity are freed.
 */
pub struct PagePool<const N: usize> {
  store: Arc<ArrayQueue<Page<N>>>,
}
impl<const N: usize> PagePool<N> {
  pub fn new(cap: usize) -> Self {
    Self {
      store: ArrayQueue::new(cap).to_arc(),
    }
  }

  pub fn acquire(&self) -> PageRef<N> {
    let backoff = Backoff::new();
    while !backoff.is_completed() {
      if let Some(page) = self.store.pop() {
        return PageRef::from_exists(self.store.clone(), page);
      }
      backoff.snooze();
    }
    PageRef::new(self.store.clone())
  }

  #[allow(unused)]
  pub fn len(&self) -> usize {
    self.store.len()
  }
}

#[cfg(test)]
#[path = "tests/page_pool.rs"]
mod tests;
