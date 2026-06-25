use std::{
  mem::ManuallyDrop,
  ops::{Deref, DerefMut},
};

use crossbeam::{queue::ArrayQueue, utils::Backoff};

use super::Page;
use crate::utils::SBox;

/**
 * Owned handle to a pooled page.
 *
 * `PageRef` keeps the page in `ManuallyDrop` because dropping the handle should
 * normally return the page to the pool instead of freeing it. If the pool is
 * already full, the failed `push` drops the page normally and releases the
 * allocation.
 */
pub struct PageRef<const N: usize> {
  page: ManuallyDrop<Page<N>>,
  store: SBox<ArrayQueue<Page<N>>>,
}
impl<const N: usize> PageRef<N> {
  const fn from_exists(store: SBox<ArrayQueue<Page<N>>>, page: Page<N>) -> Self {
    Self {
      page: ManuallyDrop::new(page),
      store,
    }
  }

  fn new(store: SBox<ArrayQueue<Page<N>>>) -> Self {
    Self::from_exists(store, Page::new())
  }
}
impl<const N: usize> Deref for PageRef<N> {
  type Target = Page<N>;

  #[inline]
  fn deref(&self) -> &Self::Target {
    self.page.deref()
  }
}
impl<const N: usize> DerefMut for PageRef<N> {
  #[inline]
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.page.deref_mut()
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
 * Bounded pool of reusable aligned pages.
 *
 * `PagePool` reduces heap allocation churn for direct-I/O pages. `acquire`
 * returns a recycled page when one is available and allocates a new page only
 * when the pool is empty. When the returned `PageRef` is dropped, the page is
 * returned to the pool if there is capacity.
 */
pub struct PagePool<const N: usize> {
  store: SBox<ArrayQueue<Page<N>>>,
}
impl<const N: usize> PagePool<N> {
  pub fn new(cap: usize) -> Self {
    Self {
      store: SBox::new(ArrayQueue::new(cap)),
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
