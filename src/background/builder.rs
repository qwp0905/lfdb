use std::time::Duration;

use super::{
  BufferingThread, Fallback, IntervalWorkThread, PreloadThread, SingleFn, ThreadPool,
};

const DEFAULT_STACK_SIZE: usize = 64 << 10;

/**
 * Builders for the engine's background runtime families.
 *
 * Runtime construction is split into two layers. `ThreadBuilder` stores common
 * thread settings such as name and stack size, then selects either the
 * multi-threaded family or the single-threaded family. Multi-threaded runtimes
 * use `SharedFn`; single-threaded runtimes use `SingleFn`.
 */
pub struct ThreadBuilder {
  name: String,
  stack_size: usize,
}
impl ThreadBuilder {
  pub const fn new() -> Self {
    ThreadBuilder {
      name: String::new(),
      stack_size: DEFAULT_STACK_SIZE,
    }
  }
  pub fn name<S: ToString>(mut self, name: S) -> Self {
    self.name = name.to_string();
    self
  }
  #[allow(unused)]
  pub const fn stack_size(mut self, size: usize) -> Self {
    self.stack_size = size;
    self
  }
  pub fn multi(self, count: usize) -> ThreadPool {
    ThreadPool::new(self.name, self.stack_size, count)
  }
  pub const fn single(self) -> SingleThreadBuilder {
    SingleThreadBuilder { builder: self }
  }
}

pub struct SingleThreadBuilder {
  builder: ThreadBuilder,
}
impl SingleThreadBuilder {
  pub fn interval<T, R, F>(self, timeout: Duration, f: F) -> IntervalWorkThread<T, R>
  where
    T: Send + 'static,
    R: Send + 'static,
    F: FnMut(Option<T>) -> R + Send + Sync + 'static,
  {
    IntervalWorkThread::new(
      self.builder.name,
      self.builder.stack_size,
      timeout,
      SingleFn::new(f),
    )
  }

  pub fn buffering<F, T, R>(self, count: usize, when_buffered: F) -> BufferingThread<T, R>
  where
    T: Send + 'static,
    R: Send + Clone + 'static,
    F: FnMut(Vec<T>) -> R + Send + Sync + 'static,
  {
    BufferingThread::new(
      self.builder.name,
      self.builder.stack_size,
      count,
      SingleFn::new(when_buffered),
    )
  }

  pub fn preload<T, F, G>(
    self,
    timeout: Duration,
    preload: F,
    fallback: G,
  ) -> PreloadThread<T>
  where
    T: Send + 'static,
    F: FnMut(()) -> T + Send + 'static,
    G: FnMut(Fallback<T>) + Send + 'static,
  {
    PreloadThread::new(
      self.builder.name,
      self.builder.stack_size,
      timeout,
      SingleFn::new(preload),
      SingleFn::new(fallback),
    )
  }
}
