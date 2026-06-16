use std::{sync::Arc, time::Duration};

use super::{
  BackgroundThread, EagerBufferingThread, IntervalWorkThread, PreloadThread, SharedFn,
  SharedWorkThread, SingleFn,
};

const DEFAULT_STACK_SIZE: usize = 64 << 10;

pub struct WorkBuilder {
  name: String,
  stack_size: usize,
}
impl WorkBuilder {
  pub const fn new() -> Self {
    WorkBuilder {
      name: String::new(),
      stack_size: DEFAULT_STACK_SIZE,
    }
  }
  pub fn name<S: ToString>(mut self, name: S) -> Self {
    self.name = name.to_string();
    self
  }
  #[allow(dead_code)]
  pub const fn stack_size(mut self, size: usize) -> Self {
    self.stack_size = size;
    self
  }
  pub const fn multi(self, count: usize) -> MultiThreadBuilder {
    MultiThreadBuilder {
      builder: self,
      count,
    }
  }
  pub const fn single(self) -> SingleThreadBuilder {
    SingleThreadBuilder { builder: self }
  }
}
pub struct MultiThreadBuilder {
  builder: WorkBuilder,
  count: usize,
}
impl MultiThreadBuilder {
  pub fn shared<T, R, F>(self, build: F) -> SharedWorkThread<T, R>
  where
    T: Send + 'static,
    R: Send + 'static,
    F: Fn(T) -> R + Send + Sync + 'static,
  {
    SharedWorkThread::new(
      self.builder.name,
      self.builder.stack_size,
      self.count,
      SharedFn::new(Arc::new(build)),
    )
  }
}

pub struct SingleThreadBuilder {
  builder: WorkBuilder,
}
impl SingleThreadBuilder {
  pub fn interval<T, R, F>(self, timeout: Duration, f: F) -> impl BackgroundThread<T, R>
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

  pub fn eager_buffering<F, T, R>(
    self,
    count: usize,
    when_buffered: F,
  ) -> impl BackgroundThread<T, R>
  where
    T: Send + 'static,
    R: Send + Clone + 'static,
    F: FnMut(Vec<T>) -> R + Send + Sync + 'static,
  {
    EagerBufferingThread::new(
      self.builder.name,
      self.builder.stack_size,
      count,
      SingleFn::new(when_buffered),
    )
  }

  pub fn preload<T, F, R>(
    self,
    timeout: Duration,
    preload: F,
    fallback: R,
  ) -> impl BackgroundThread<(), T>
  where
    T: Send + 'static,
    F: FnMut(()) -> T + Send + Sync + 'static,
    R: FnMut(Option<T>) + Send + Sync + 'static,
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
