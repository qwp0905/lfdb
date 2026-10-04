use std::{
  cell::{OnceCell, RefCell},
  mem::ManuallyDrop,
  ops::{Deref, DerefMut},
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Weak,
  },
  thread::LocalKey,
};

use crossbeam::{
  deque::{Injector, Worker},
  utils::Backoff,
};

use super::Page;

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
  global: Arc<GlobalQueue<N>>,
}
impl<const N: usize> PageRef<N> {
  const fn from_exists(global: Arc<GlobalQueue<N>>, page: Page<N>) -> Self {
    Self {
      page: ManuallyDrop::new(page),
      global,
    }
  }

  fn new(store: Arc<GlobalQueue<N>>) -> Self {
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
    let page = unsafe { ManuallyDrop::take(&mut self.page) };
    self.global.store(page);
  }
}

macro_rules! create_page_pool {
  ($capacity:expr, $size:ty $(,)?) => {{
    use crate::disk::{LocalPool, PagePool};
    thread_local! {
      static LOCAL: LocalPool<$size> = const { LocalPool::new() };
    }
    PagePool::new($capacity, &LOCAL)
  }};
}
pub(crate) use create_page_pool;

/**
 * Bounded pool of reusable aligned pages.
 *
 * `PagePool` reduces heap allocation churn for direct-I/O pages. `acquire`
 * returns a recycled page when one is available and allocates a new page only
 * when the pool is empty. When the returned `PageRef` is dropped, the page is
 * returned to the pool if there is capacity.
 */
pub struct PagePool<const N: usize> {
  global: Arc<GlobalQueue<N>>,
  local: &'static LocalKey<LocalPool<N>>,
}
impl<const N: usize> PagePool<N> {
  pub fn new(cap: usize, local: &'static LocalKey<LocalPool<N>>) -> Self {
    Self {
      global: Arc::new(GlobalQueue::new(cap)),
      local,
    }
  }

  fn create_local(&self) -> RefCell<LocalQueue<N>> {
    RefCell::new(LocalQueue::new(Arc::downgrade(&self.global)))
  }

  pub fn acquire(&self) -> PageRef<N> {
    self.local.with(|v| {
      let mut local = v.get_or_init(|| self.create_local()).borrow_mut();
      if local.global.as_ptr().addr() != Arc::as_ptr(&self.global).addr() {
        self
          .global
          .idle_count
          .fetch_add(local.queue.len(), Ordering::Relaxed);
        local.global = Arc::downgrade(&self.global);
      }

      if let Some(page) = local.pop() {
        self.global.idle_count.fetch_sub(1, Ordering::Relaxed);
        return PageRef::from_exists(self.global.clone(), page);
      };

      let backoff = Backoff::new();
      while !backoff.is_completed() {
        if let Some(page) = self.global.pop_batch_with(&local) {
          self.global.idle_count.fetch_sub(1, Ordering::Relaxed);
          return PageRef::from_exists(self.global.clone(), page);
        }
        backoff.snooze();
      }
      PageRef::new(self.global.clone())
    })
  }

  #[cfg(test)]
  pub fn len(&self) -> usize {
    self.global.idle_count.load(Ordering::Relaxed)
  }
}

const BATCH_SIZE: usize = 4;

pub type LocalPool<const N: usize> = OnceCell<RefCell<LocalQueue<N>>>;

pub struct LocalQueue<const N: usize> {
  queue: Worker<Page<N>>,
  global: Weak<GlobalQueue<N>>,
}
impl<const N: usize> LocalQueue<N> {
  fn new(global: Weak<GlobalQueue<N>>) -> Self {
    Self {
      queue: Worker::new_fifo(),
      global,
    }
  }
  fn pop(&self) -> Option<Page<N>> {
    self.queue.pop()
  }
}
impl<const N: usize> Drop for LocalQueue<N> {
  fn drop(&mut self) {
    let Some(global) = self.global.upgrade() else {
      return;
    };
    while let Some(page) = self.queue.pop() {
      global.queue.push(page);
    }
  }
}

struct GlobalQueue<const N: usize> {
  queue: Injector<Page<N>>,
  idle_count: AtomicUsize,
  capacity: usize,
}
impl<const N: usize> GlobalQueue<N> {
  fn new(capacity: usize) -> Self {
    Self {
      queue: Injector::new(),
      idle_count: AtomicUsize::new(0),
      capacity,
    }
  }
  fn store(&self, page: Page<N>) {
    if self.idle_count.fetch_add(1, Ordering::Relaxed) < self.capacity {
      self.queue.push(page);
      return;
    };
    self.idle_count.fetch_sub(1, Ordering::Relaxed);
  }

  fn pop_batch_with(&self, local: &LocalQueue<N>) -> Option<Page<N>> {
    self
      .queue
      .steal_batch_with_limit_and_pop(&local.queue, BATCH_SIZE)
      .success()
  }
}

#[cfg(test)]
#[path = "tests/page_pool.rs"]
mod tests;
