use std::{
  cell::{OnceCell, RefCell},
  mem::{ManuallyDrop, MaybeUninit},
  ops::{Deref, DerefMut},
  sync::{Arc, Weak},
  thread::LocalKey,
};

use crossbeam::{queue::ArrayQueue, utils::Backoff};

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
  strategy: Strategy<N>,
}
impl<const N: usize> PageRef<N> {
  const fn from_exists(strategy: Strategy<N>, page: Page<N>) -> Self {
    Self {
      page: ManuallyDrop::new(page),
      strategy,
    }
  }

  fn new(strategy: Strategy<N>) -> Self {
    Self::from_exists(strategy, Page::new())
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
    self.strategy.store(page);
  }
}

macro_rules! create_page_pool {
  ($capacity:expr, $size:ty $(,)?) => {{
    use crate::disk::{LocalPool, PagePool};
    thread_local! {
      static LOCAL: LocalPool<$size> = const { LocalPool::new() };
    }
    PagePool::with_local($capacity, &LOCAL)
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
  strategy: Strategy<N>,
}
impl<const N: usize> PagePool<N> {
  pub fn with_local(cap: usize, local: &'static LocalKey<LocalPool<N>>) -> Self {
    let queue = ArrayQueue::new(cap);
    for _ in 0..cap {
      let _ = queue.push(Page::new());
    }
    Self {
      strategy: Strategy::LocalCached(Arc::new(queue), local),
    }
  }
  pub fn new(cap: usize) -> Self {
    let queue = ArrayQueue::new(cap);
    for _ in 0..cap {
      let _ = queue.push(Page::new());
    }
    Self {
      strategy: Strategy::Global(Arc::new(queue)),
    }
  }

  pub fn create_new(&self) -> PageRef<N> {
    PageRef::new(self.strategy.clone())
  }

  pub fn acquire(&self) -> PageRef<N> {
    if let Some(page) = self.strategy.pop() {
      return PageRef::from_exists(self.strategy.clone(), page);
    }
    self.create_new()
  }
}

enum Strategy<const N: usize> {
  Global(Arc<ArrayQueue<Page<N>>>),
  LocalCached(Arc<ArrayQueue<Page<N>>>, &'static LocalKey<LocalPool<N>>),
}
impl<const N: usize> Strategy<N> {
  fn create_local(
    &self,
    global: &Arc<ArrayQueue<Page<N>>>,
  ) -> RefCell<LocalPageQueue<N>> {
    RefCell::new(LocalPageQueue::new(
      Arc::downgrade(global),
      global.capacity(),
    ))
  }
  fn store(&self, page: Page<N>) {
    match self {
      Self::Global(queue) => {
        let _ = queue.push(page);
      }
      Self::LocalCached(queue, local) => local.with(|v| {
        let mut local = v.get_or_init(|| self.create_local(queue)).borrow_mut();
        if local.global.as_ptr().addr() != Arc::as_ptr(queue).addr() {
          local.global = Arc::downgrade(queue);
        }
        if let Err(err) = local.push(page) {
          let _ = queue.push(err);
        }
      }),
    }
  }

  fn pop(&self) -> Option<Page<N>> {
    match self {
      Self::Global(queue) => {
        let backoff = Backoff::new();
        while !backoff.is_completed() {
          if let Some(page) = queue.pop() {
            return Some(page);
          }
          backoff.snooze();
        }
        None
      }
      Self::LocalCached(queue, local) => local.with(|v| {
        let mut local = v.get_or_init(|| self.create_local(queue)).borrow_mut();
        if local.global.as_ptr().addr() != Arc::as_ptr(queue).addr() {
          local.global = Arc::downgrade(queue);
        }
        local.pop().or_else(|| queue.pop())
      }),
    }
  }
}
impl<const N: usize> Clone for Strategy<N> {
  fn clone(&self) -> Self {
    match self {
      Self::Global(queue) => Self::Global(queue.clone()),
      Self::LocalCached(queue, local) => Self::LocalCached(queue.clone(), *local),
    }
  }
}

pub type LocalPool<const N: usize> = OnceCell<RefCell<LocalPageQueue<N>>>;

pub struct LocalPageQueue<const N: usize> {
  queue: RingBuffer<Page<N>>,
  global: Weak<ArrayQueue<Page<N>>>,
}
impl<const N: usize> LocalPageQueue<N> {
  fn new(global: Weak<ArrayQueue<Page<N>>>, capacity: usize) -> Self {
    let queue = RingBuffer::new(capacity);
    Self { queue, global }
  }
  fn pop(&mut self) -> Option<Page<N>> {
    self.queue.pop()
  }
  fn push(&mut self, page: Page<N>) -> std::result::Result<(), Page<N>> {
    self.queue.push(page)
  }
}
impl<const N: usize> Drop for LocalPageQueue<N> {
  fn drop(&mut self) {
    let Some(global) = self.global.upgrade() else {
      return;
    };
    while let Some(page) = self.queue.pop() {
      let _ = global.push(page);
    }
  }
}

struct RingBuffer<T> {
  head: usize,
  len: usize,
  slots: Box<[MaybeUninit<T>]>,
}
impl<T> RingBuffer<T> {
  fn new(cap: usize) -> Self {
    let mut slots = Vec::with_capacity(cap);
    slots.resize_with(cap, MaybeUninit::uninit);
    Self {
      head: 0,
      len: 0,
      slots: slots.into_boxed_slice(),
    }
  }
  const fn is_full(&self) -> bool {
    self.len == self.slots.len()
  }
  const fn is_empty(&self) -> bool {
    self.len == 0
  }
  fn push(&mut self, value: T) -> std::result::Result<(), T> {
    if self.is_full() {
      return Err(value);
    }
    self.slots[(self.head + self.len) % self.slots.len()].write(value);
    self.len += 1;
    Ok(())
  }
  fn pop(&mut self) -> Option<T> {
    if self.is_empty() {
      return None;
    }
    let value = unsafe { self.slots[self.head].assume_init_read() };
    self.head = (self.head + 1) % self.slots.len();
    self.len -= 1;
    Some(value)
  }
}
impl<T> Drop for RingBuffer<T> {
  fn drop(&mut self) {
    for i in 0..self.len {
      unsafe { self.slots[(self.head + i) % self.slots.len()].assume_init_drop() };
    }
  }
}

#[cfg(test)]
#[path = "tests/page_pool.rs"]
mod tests;
