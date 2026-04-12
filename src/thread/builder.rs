use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  thread::Builder,
  time::Duration,
};

use super::{
  BackgroundThread, EagerBufferingThread, IntervalWorkThread, LazyBufferingThread,
  OnceHandle, SharedWorkThread, SingleFn,
};

const DEFAULT_STACK_SIZE: usize = 64 << 10;

pub fn once<F, T>(f: F) -> OnceHandle<T>
where
  T: Send + 'static,
  F: FnOnce() -> T + Send + 'static,
{
  let handle = Builder::new()
    .stack_size(DEFAULT_STACK_SIZE)
    .spawn(f)
    .unwrap();
  OnceHandle::new(handle)
}

pub struct WorkBuilder {
  name: String,
  stack_size: usize,
}
impl WorkBuilder {
  pub fn new() -> Self {
    WorkBuilder {
      name: Default::default(),
      stack_size: DEFAULT_STACK_SIZE,
    }
  }
  pub fn name<S: ToString>(mut self, name: S) -> Self {
    self.name = name.to_string();
    self
  }
  #[allow(dead_code)]
  pub fn stack_size(mut self, size: usize) -> Self {
    self.stack_size = size;
    self
  }
  pub fn multi(self, count: usize) -> MultiThreadBuilder {
    MultiThreadBuilder {
      builder: self,
      count,
    }
  }
  pub fn single(self) -> SingleThreadBuilder {
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
    T: Send + UnwindSafe + 'static,
    R: Send + 'static,
    F: Fn(T) -> R + RefUnwindSafe + Send + Sync + 'static,
  {
    SharedWorkThread::new(
      self.builder.name,
      self.builder.stack_size,
      self.count,
      build,
    )
  }
}

pub struct SingleThreadBuilder {
  builder: WorkBuilder,
}
impl SingleThreadBuilder {
  pub fn interval<T, R, F>(self, timeout: Duration, f: F) -> impl BackgroundThread<T, R>
  where
    T: Send + UnwindSafe + RefUnwindSafe + 'static,
    R: Send + 'static,
    F: FnMut(Option<T>) -> R + Send + RefUnwindSafe + Sync + 'static,
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
    T: Send + UnwindSafe + 'static,
    R: Send + Clone + 'static,
    F: FnMut(Vec<T>) -> R + RefUnwindSafe + Send + Sync + 'static,
  {
    EagerBufferingThread::new(
      self.builder.name,
      self.builder.stack_size,
      count,
      SingleFn::new(when_buffered),
    )
  }

  pub fn lazy_buffering<T, R, F>(
    self,
    timeout: Duration,
    count: usize,
    when_buffered: F,
  ) -> impl BackgroundThread<T, R>
  where
    T: Send + UnwindSafe + RefUnwindSafe + 'static,
    R: Send + Clone + 'static,
    F: FnMut(Vec<T>) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    LazyBufferingThread::new(
      self.builder.name,
      self.builder.stack_size,
      count,
      timeout,
      SingleFn::new(when_buffered),
    )
  }
}
