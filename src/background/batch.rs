use std::sync::{
  atomic::{fence, AtomicBool, Ordering},
  Arc,
};

use crossbeam::queue::SegQueue;

use super::{ThreadPool, UnsafeFn};

struct BatchQueue<T> {
  occupied: AtomicBool,
  buffered: SegQueue<T>,
}
impl<T> BatchQueue<T> {
  const fn new() -> Self {
    Self {
      occupied: AtomicBool::new(false),
      buffered: SegQueue::new(),
    }
  }
}
pub struct BatchExecutor<T> {
  pool: Arc<ThreadPool>,
  handler: UnsafeFn<Vec<T>, ()>,
  queue: Arc<BatchQueue<T>>,
  max_count: usize,
}
impl<T> BatchExecutor<T> {
  pub fn new<F>(pool: Arc<ThreadPool>, handler: F, max_count: usize) -> Self
  where
    F: FnMut(Vec<T>) + Send + 'static,
  {
    Self {
      pool,
      handler: UnsafeFn::new(handler),
      queue: Arc::new(BatchQueue::new()),
      max_count,
    }
  }

  pub fn dispatch(&self, value: T)
  where
    T: Send + 'static,
  {
    self.queue.buffered.push(value);
    if self.queue.occupied.fetch_or(true, Ordering::Relaxed) {
      return;
    }

    fence(Ordering::Acquire);
    let pool = self.pool.clone();
    let queue = self.queue.clone();
    let handler = self.handler.clone();
    let count = self.max_count;
    self
      .pool
      .spawn(move || Self::drain(pool, queue, handler, count));
  }

  fn drain(
    pool: Arc<ThreadPool>,
    queue: Arc<BatchQueue<T>>,
    handler: UnsafeFn<Vec<T>, ()>,
    count: usize,
  ) where
    T: Send + 'static,
  {
    let mut values = Vec::with_capacity(count);
    for input in (0..count).map_while(|_| queue.buffered.pop()) {
      values.push(input);
    }

    if !values.is_empty() {
      unsafe { handler.call(values) };
    }

    queue.occupied.fetch_and(false, Ordering::Release);
    if queue.buffered.is_empty() {
      return;
    }
    if queue.occupied.fetch_or(true, Ordering::Relaxed) {
      return;
    }

    fence(Ordering::Acquire);
    pool
      .clone()
      .spawn(move || Self::drain(pool, queue, handler, count));
  }
}
