use std::{
  iter::{from_fn, repeat},
  ops::Deref,
  sync::{atomic::Ordering, Arc},
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
    if self.try_occupy() {
      return Some(Occupied::new(queue(), Some(value)));
    }
    self.queue.push(value);
    self.try_occupy().then(|| Occupied::new(queue(), None))
  }

  fn try_occupy(&self) -> bool {
    !self.occupied.fetch_or(true, Ordering::AcqRel)
  }

  fn try_release(&self) -> bool {
    self.occupied.fetch_and(false, Ordering::AcqRel);
    if self.queue.is_empty() {
      return true;
    }
    self.occupied.fetch_or(true, Ordering::Acquire)
  }

  unsafe fn drain(&self) -> impl Iterator<Item = T> + '_ {
    repeat(()).map_while(|_| unsafe { self.queue.pop() })
  }
}
impl<T> Default for BatchQueue<T> {
  fn default() -> Self {
    Self::new()
  }
}

/**
 * Proof that the caller owns the batch, so only it may drain the queue. The
 * value that won without being queued may trail an earlier value from the same
 * thread, so it is drained only once the queue runs dry, or queued on release.
 */
#[must_use]
pub struct Occupied<Q, T>
where
  Q: Deref<Target = BatchQueue<T>>,
{
  queue: Q,
  own: Option<T>,
}
impl<Q, T> Occupied<Q, T>
where
  Q: Deref<Target = BatchQueue<T>>,
{
  const fn new(queue: Q, own: Option<T>) -> Self {
    Self { queue, own }
  }

  pub fn drain_n(&mut self, count: usize) -> impl Iterator<Item = T> + '_ {
    self.drain_all().take(count)
  }

  pub fn drain_all(&mut self) -> impl Iterator<Item = T> + '_ {
    unsafe { self.queue.drain() }.chain(from_fn(|| self.own.take()))
  }

  pub fn try_release(&mut self) -> bool {
    if let Some(own) = self.own.take() {
      self.queue.queue.push(own);
    }
    self.queue.try_release()
  }
}

#[cfg(all(test, loom))]
#[path = "tests/batch_loom.rs"]
mod tests;
