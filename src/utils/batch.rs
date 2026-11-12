use std::{
  iter::repeat,
  mem::ManuallyDrop,
  ops::Deref,
  sync::{atomic::Ordering, Arc},
  thread::panicking,
};

use super::{loom::AtomicBool, MpscQueue};

pub struct BatchQueue<T> {
  queue: MpscQueue<T>,
  occupied: AtomicBool,
}
impl<T> BatchQueue<T> {
  pub fn new() -> Self {
    Self {
      queue: MpscQueue::new(),
      occupied: AtomicBool::new(false),
    }
  }

  pub fn compete(&self, value: T) -> Option<Occupied<&Self, T>> {
    self.compete_by(value, || self)
  }

  pub fn compete_owned(self: &Arc<Self>, value: T) -> Option<Occupied<Arc<Self>, T>> {
    self.compete_by(value, || self.clone())
  }

  fn compete_by<Q>(&self, value: T, queue: impl FnOnce() -> Q) -> Option<Occupied<Q, T>>
  where
    Q: Deref<Target = Self>,
  {
    self.queue.push(value);
    (!self.occupied.fetch_or(true, Ordering::AcqRel)).then(|| Occupied::new(queue()))
  }
  unsafe fn drain(&self) -> impl Iterator<Item = T> + '_ {
    repeat(()).map_while(|_| unsafe { self.queue.pop() })
  }

  fn try_release(&self) -> bool {
    self.occupied.fetch_and(false, Ordering::AcqRel);
    if self.queue.is_empty() {
      return true;
    }
    self.occupied.fetch_or(true, Ordering::Acquire)
  }
}
impl<T> Default for BatchQueue<T> {
  fn default() -> Self {
    Self::new()
  }
}

/**
 * Proof that the caller owns the batch, so only it may drain the queue.
 */
#[must_use]
pub struct Occupied<Q, T>(ManuallyDrop<Q>)
where
  Q: Deref<Target = BatchQueue<T>>;

impl<Q, T> Occupied<Q, T>
where
  Q: Deref<Target = BatchQueue<T>>,
{
  const fn new(queue: Q) -> Self {
    Self(ManuallyDrop::new(queue))
  }

  pub fn drain_n(&mut self, count: usize) -> impl Iterator<Item = T> + '_ {
    self.drain_all().take(count)
  }

  pub fn drain_all(&mut self) -> impl Iterator<Item = T> + '_ {
    unsafe { self.0.drain() }
  }

  /**
   * Gives up ownership, or returns it when items arrived that no one else will
   * drain.
   */
  pub fn release(self) -> Option<Self> {
    let mut this = ManuallyDrop::new(self);
    let queue = unsafe { ManuallyDrop::take(&mut this.0) };
    (!queue.try_release()).then(|| Self::new(queue))
  }
}
impl<Q, T> Drop for Occupied<Q, T>
where
  Q: Deref<Target = BatchQueue<T>>,
{
  fn drop(&mut self) {
    unsafe { ManuallyDrop::drop(&mut self.0) };
    debug_assert!(panicking());
  }
}

#[cfg(all(test, loom))]
#[path = "tests/batch_loom.rs"]
mod tests;
