use std::sync::{
  atomic::{AtomicBool, Ordering},
  Arc,
};

use crossbeam::queue::SegQueue;

use super::{oneshot, Oneshot, OneshotFulfill, ThreadPool, UnsafeFn};

struct BatchQueue<T, R> {
  occupied: AtomicBool,
  buffered: SegQueue<(T, OneshotFulfill<R>)>,
}
impl<T, R> BatchQueue<T, R> {
  const fn new() -> Self {
    Self {
      occupied: AtomicBool::new(false),
      buffered: SegQueue::new(),
    }
  }
}
pub struct BatchExecutor<T, R> {
  pool: Arc<ThreadPool>,
  handler: UnsafeFn<Vec<T>, R>,
  queue: Arc<BatchQueue<T, R>>,
  max_count: usize,
}
impl<T, R> BatchExecutor<T, R> {
  pub fn new<F>(pool: Arc<ThreadPool>, handler: F, max_count: usize) -> Self
  where
    F: FnMut(Vec<T>) -> R + Send + 'static,
  {
    Self {
      pool,
      handler: UnsafeFn::new(handler),
      queue: Arc::new(BatchQueue::new()),
      max_count,
    }
  }

  pub fn execute(&self, value: T) -> Oneshot<R>
  where
    T: Send + 'static,
    R: Clone + Send + 'static,
  {
    let (o, f) = oneshot();
    self.queue.buffered.push((value, f));
    if !self.queue.occupied.fetch_or(true, Ordering::AcqRel) {
      let pool = self.pool.clone();
      let queue = self.queue.clone();
      let handler = self.handler.clone();
      let count = self.max_count;
      self
        .pool
        .spawn(move || Self::drain(pool, queue, handler, count));
    }
    o
  }

  fn drain(
    pool: Arc<ThreadPool>,
    queue: Arc<BatchQueue<T, R>>,
    handler: UnsafeFn<Vec<T>, R>,
    count: usize,
  ) where
    T: Send + 'static,
    R: Clone + Send + 'static,
  {
    let mut values = Vec::with_capacity(count);
    let mut waiting = Vec::with_capacity(count);
    for (input, done) in (0..count).map_while(|_| queue.buffered.pop()) {
      values.push(input);
      waiting.push(done);
    }

    if !values.is_empty() {
      let result = unsafe { handler.call(values) };
      waiting
        .into_iter()
        .for_each(|done| done.fulfill(result.clone()));
    }

    queue.occupied.fetch_and(false, Ordering::Release);
    if queue.buffered.is_empty() {
      return;
    }
    if queue.occupied.fetch_or(true, Ordering::AcqRel) {
      return;
    }

    pool
      .clone()
      .spawn(move || Self::drain(pool, queue, handler, count));
  }
}
