use std::{
  cell::{OnceCell, RefCell, UnsafeCell},
  mem::{ManuallyDrop, MaybeUninit},
  ops::{Deref, DerefMut},
  sync::{
    atomic::{fence, AtomicUsize, Ordering},
    Arc, Weak,
  },
  thread::LocalKey,
};

use crossbeam::utils::{Backoff, CachePadded};

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
  ($capacity:expr, $size:ty, $batch_size:expr $(,)?) => {{
    use crate::disk::{LocalPool, PagePool};
    thread_local! {
      static LOCAL: LocalPool<$size> = const { LocalPool::new() };
    }
    PagePool::with_local($capacity, &LOCAL, $batch_size)
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
  local: Option<(&'static LocalKey<LocalPool<N>>, usize)>,
}
impl<const N: usize> PagePool<N> {
  pub fn with_local(
    cap: usize,
    local: &'static LocalKey<LocalPool<N>>,
    batch_size: usize,
  ) -> Self {
    Self {
      global: Arc::new(GlobalQueue::new(cap)),
      local: Some((local, batch_size)),
    }
  }
  pub fn new(cap: usize) -> Self {
    Self {
      global: Arc::new(GlobalQueue::new(cap)),
      local: None,
    }
  }

  fn create_local(&self, batch_size: usize) -> RefCell<LocalPageQueue<N>> {
    RefCell::new(LocalPageQueue::new(
      Arc::downgrade(&self.global),
      batch_size,
    ))
  }

  fn pop_with_local(
    &self,
    local: &'static LocalKey<LocalPool<N>>,
    batch_size: usize,
  ) -> PageRef<N> {
    local.with(|v| {
      let mut local = v.get_or_init(|| self.create_local(batch_size)).borrow_mut();
      if local.global.as_ptr().addr() != Arc::as_ptr(&self.global).addr() {
        local.global = Arc::downgrade(&self.global);
      }

      if let Some(page) = local.pop() {
        return PageRef::from_exists(self.global.clone(), page);
      };

      let backoff = Backoff::new();
      while !backoff.is_completed() {
        if let Some(page) = self.global.pop_batch_with(&mut local) {
          return PageRef::from_exists(self.global.clone(), page);
        }
        backoff.snooze();
      }
      PageRef::new(self.global.clone())
    })
  }

  pub fn acquire(&self) -> PageRef<N> {
    if let Some((local, batch_size)) = self.local {
      return self.pop_with_local(local, batch_size);
    }

    let backoff = Backoff::new();
    while !backoff.is_completed() {
      if let Some(page) = self.global.pop() {
        return PageRef::from_exists(self.global.clone(), page);
      };
      backoff.snooze();
    }
    PageRef::new(self.global.clone())
  }

  #[cfg(test)]
  pub fn len(&self) -> usize {
    self.global.queue.len()
  }
}

pub type LocalPool<const N: usize> = OnceCell<RefCell<LocalPageQueue<N>>>;

pub struct LocalPageQueue<const N: usize> {
  queue: RingBuffer<Page<N>>,
  global: Weak<GlobalQueue<N>>,
}
impl<const N: usize> LocalPageQueue<N> {
  fn new(global: Weak<GlobalQueue<N>>, batch_size: usize) -> Self {
    Self {
      queue: RingBuffer::new(batch_size - 1),
      global,
    }
  }
  fn pop(&mut self) -> Option<Page<N>> {
    self.queue.pop()
  }
}
impl<const N: usize> Drop for LocalPageQueue<N> {
  fn drop(&mut self) {
    let Some(global) = self.global.upgrade() else {
      return;
    };
    while let Some(page) = self.queue.pop() {
      global.store(page);
    }
  }
}

struct GlobalQueue<const N: usize> {
  queue: AtomicRingBuffer<Page<N>>,
}
impl<const N: usize> GlobalQueue<N> {
  fn new(capacity: usize) -> Self {
    Self {
      queue: AtomicRingBuffer::new(capacity),
    }
  }
  fn store(&self, page: Page<N>) {
    let _ = self.queue.push(page);
  }

  fn pop_batch_with(&self, local: &mut LocalPageQueue<N>) -> Option<Page<N>> {
    self.queue.pop_batch_with(&mut local.queue)
  }
  fn pop(&self) -> Option<Page<N>> {
    self.queue.pop()
  }
}

struct RingBuffer<T> {
  head: usize,
  len: usize,
  cap: usize,
  slots: Box<[MaybeUninit<T>]>,
}
impl<T> RingBuffer<T> {
  fn new(cap: usize) -> Self {
    let slots = (0..cap).map(|_| MaybeUninit::uninit()).collect();
    Self {
      head: 0,
      len: 0,
      cap,
      slots,
    }
  }
  const fn is_full(&self) -> bool {
    self.len == self.cap
  }
  const fn is_empty(&self) -> bool {
    self.len == 0
  }
  fn push(&mut self, value: T) -> std::result::Result<(), T> {
    if self.is_full() {
      return Err(value);
    }
    self.slots[(self.head + self.len) % self.cap].write(value);
    self.len += 1;
    Ok(())
  }
  fn pop(&mut self) -> Option<T> {
    if self.is_empty() {
      return None;
    }
    let value = unsafe { self.slots[self.head].assume_init_read() };
    self.head = (self.head + 1) % self.cap;
    self.len -= 1;
    Some(value)
  }
  const fn sparse_len(&self) -> usize {
    self.cap - self.len
  }
}
impl<T> Drop for RingBuffer<T> {
  fn drop(&mut self) {
    for i in 0..self.len {
      unsafe { self.slots[(self.head + i) % self.cap].assume_init_drop() };
    }
  }
}

struct Slot<T> {
  stamp: AtomicUsize,
  value: UnsafeCell<MaybeUninit<T>>,
}
impl<T> Slot<T> {
  const fn new(i: usize) -> Self {
    Self {
      stamp: AtomicUsize::new(i),
      value: UnsafeCell::new(MaybeUninit::uninit()),
    }
  }
}

struct AtomicRingBuffer<T> {
  head: CachePadded<AtomicUsize>,
  tail: CachePadded<AtomicUsize>,
  buffer: Box<[Slot<T>]>,
  cap: usize,
  one_lap: usize,
}
impl<T> AtomicRingBuffer<T> {
  fn new(cap: usize) -> Self {
    let buffer: Box<[Slot<T>]> = (0..cap).map(Slot::new).collect();
    let one_lap = (cap + 1).next_power_of_two();
    Self {
      head: CachePadded::new(AtomicUsize::new(0)),
      tail: CachePadded::new(AtomicUsize::new(0)),
      buffer,
      cap,
      one_lap,
    }
  }

  fn push_or_else<F>(&self, mut value: T, f: F) -> std::result::Result<(), T>
  where
    F: Fn(T, usize, usize, &Slot<T>) -> std::result::Result<T, T>,
  {
    let backoff = Backoff::new();
    let mut tail = self.tail.load(Ordering::Relaxed);

    loop {
      let index = tail & (self.one_lap - 1);
      let lap = tail & !(self.one_lap - 1);

      let new_tail = if index + 1 < self.cap {
        tail + 1
      } else {
        lap.wrapping_add(self.one_lap)
      };

      debug_assert!(index < self.buffer.len());
      let slot = unsafe { self.buffer.get_unchecked(index) };
      let stamp = slot.stamp.load(Ordering::Acquire);

      if tail == stamp {
        match self.tail.compare_exchange_weak(
          tail,
          new_tail,
          Ordering::SeqCst,
          Ordering::Relaxed,
        ) {
          Ok(_) => {
            unsafe { slot.value.get().write(MaybeUninit::new(value)) }
            slot.stamp.store(tail + 1, Ordering::Release);
            return Ok(());
          }
          Err(t) => {
            tail = t;
            backoff.spin();
          }
        }
      } else if stamp.wrapping_add(self.one_lap) == tail + 1 {
        fence(Ordering::SeqCst);
        value = f(value, tail, new_tail, slot)?;
        backoff.spin();
        tail = self.tail.load(Ordering::Relaxed);
      } else {
        backoff.snooze();
        tail = self.tail.load(Ordering::Relaxed);
      }
    }
  }

  fn push(&self, value: T) -> std::result::Result<(), T> {
    self.push_or_else(value, |v, tail, _, _| {
      let head = self.head.load(Ordering::Relaxed);
      if head.wrapping_add(self.one_lap) == tail {
        Err(v)
      } else {
        Ok(v)
      }
    })
  }

  fn pop(&self) -> Option<T> {
    let backoff = Backoff::new();
    let mut head = self.head.load(Ordering::Relaxed);

    loop {
      let index = head & (self.one_lap - 1);
      let lap = head & !(self.one_lap - 1);

      debug_assert!(index < self.buffer.len());
      let slot = unsafe { self.buffer.get_unchecked(index) };
      let stamp = slot.stamp.load(Ordering::Acquire);

      if head + 1 == stamp {
        let new = if index + 1 < self.cap {
          head + 1
        } else {
          lap.wrapping_add(self.one_lap)
        };

        match self.head.compare_exchange_weak(
          head,
          new,
          Ordering::SeqCst,
          Ordering::Relaxed,
        ) {
          Ok(_) => {
            let msg = unsafe { slot.value.get().read().assume_init() };
            slot
              .stamp
              .store(head.wrapping_add(self.one_lap), Ordering::Release);
            return Some(msg);
          }
          Err(h) => {
            head = h;
            backoff.spin();
          }
        }
      } else if stamp == head {
        fence(Ordering::SeqCst);
        if self.tail.load(Ordering::Relaxed) == head {
          return None;
        }
        backoff.spin();
        head = self.head.load(Ordering::Relaxed);
      } else {
        backoff.snooze();
        head = self.head.load(Ordering::Relaxed);
      }
    }
  }

  fn pop_batch_with(&self, ring: &mut RingBuffer<T>) -> Option<T> {
    let backoff = Backoff::new();
    let mut head = self.head.load(Ordering::Relaxed);
    let size = ring.sparse_len() + 1;

    loop {
      let index = head & (self.one_lap - 1);
      let lap = head & !(self.one_lap - 1);

      debug_assert!(index < self.buffer.len());
      let slot = unsafe { self.buffer.get_unchecked(index) };
      let stamp = slot.stamp.load(Ordering::Acquire);

      if head + 1 == stamp {
        let limit = size.min(self.cap - index);
        let mut count = 1;
        while count < limit {
          let slot = unsafe { self.buffer.get_unchecked(index + count) };
          let expected = head.wrapping_add(count + 1);
          if slot.stamp.load(Ordering::Acquire) != expected {
            break;
          }
          count += 1;
        }
        let new = if index + count < self.cap {
          head + count
        } else {
          lap.wrapping_add(self.one_lap)
        };

        match self.head.compare_exchange_weak(
          head,
          new,
          Ordering::SeqCst,
          Ordering::Relaxed,
        ) {
          Ok(_) => {
            for i in 1..count {
              let slot = unsafe { self.buffer.get_unchecked(i + index) };
              let value = unsafe { slot.value.get().read().assume_init() };
              slot.stamp.store(
                head.wrapping_add(i).wrapping_add(self.one_lap),
                Ordering::Release,
              );
              let _ = ring.push(value);
            }
            let msg = unsafe { slot.value.get().read().assume_init() };
            slot
              .stamp
              .store(head.wrapping_add(self.one_lap), Ordering::Release);
            return Some(msg);
          }
          Err(h) => {
            head = h;
            backoff.spin();
          }
        }
      } else if stamp == head {
        fence(Ordering::SeqCst);
        if self.tail.load(Ordering::Relaxed) == head {
          return None;
        }
        backoff.spin();
        head = self.head.load(Ordering::Relaxed);
      } else {
        backoff.snooze();
        head = self.head.load(Ordering::Relaxed);
      }
    }
  }

  #[cfg(test)]
  pub fn len(&self) -> usize {
    loop {
      let tail = self.tail.load(Ordering::SeqCst);
      let head = self.head.load(Ordering::SeqCst);

      if self.tail.load(Ordering::SeqCst) == tail {
        let hix = head & (self.one_lap - 1);
        let tix = tail & (self.one_lap - 1);

        return if hix < tix {
          tix - hix
        } else if hix > tix {
          self.cap - hix + tix
        } else if tail == head {
          0
        } else {
          self.cap
        };
      }
    }
  }
}
impl<T> Drop for AtomicRingBuffer<T> {
  fn drop(&mut self) {
    let head = *self.head.get_mut();
    let tail = *self.tail.get_mut();

    let hix = head & (self.one_lap - 1);
    let tix = tail & (self.one_lap - 1);

    let len = if hix < tix {
      tix - hix
    } else if hix > tix {
      self.cap - hix + tix
    } else if tail == head {
      0
    } else {
      self.cap
    };

    for i in 0..len {
      let index = if hix + i < self.cap {
        hix + i
      } else {
        hix + i - self.cap
      };

      unsafe {
        debug_assert!(index < self.buffer.len());
        let slot = self.buffer.get_unchecked_mut(index);
        (*slot.value.get()).assume_init_drop();
      }
    }
  }
}
unsafe impl<T> Send for AtomicRingBuffer<T> {}
unsafe impl<T> Sync for AtomicRingBuffer<T> {}

#[cfg(test)]
#[path = "tests/page_pool.rs"]
mod tests;
