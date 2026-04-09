use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  sync::Arc,
  time::Duration,
};

use crate::utils::ToArc;

use super::{
  BackgroundThread, EagerBufferingThread, IntervalWorkThread, LazyBufferingThread,
  SharedFn, Spawner, SubPool, TaskHandle, TimerThread,
};

pub struct RuntimeConfig {
  pub count: usize,
}

const DEFAULT_STACK_SIZE: usize = 64 << 10;

pub struct Runtime {
  spawner: Arc<Spawner>,
  timer: Arc<TimerThread>,
}
impl Runtime {
  #[inline]
  pub fn new(config: RuntimeConfig) -> Self {
    let spawner = Spawner::new(DEFAULT_STACK_SIZE, config.count).to_arc();
    let timer = TimerThread::new(spawner.clone()).to_arc();
    Self { spawner, timer }
  }

  #[inline]
  pub fn submit<T, F>(&self, task: F) -> TaskHandle<T>
  where
    T: Send + 'static,
    F: FnOnce() -> T + Send + UnwindSafe + 'static,
  {
    self.spawner.spawn(task)
  }

  pub fn reserve_submit<T, F>(&self, task: F, timeout: Duration) -> TaskHandle<T>
  where
    T: Send + 'static,
    F: FnOnce() -> T + Send + UnwindSafe + 'static,
  {
    self.timer.register(task, timeout)
  }

  #[inline]
  pub fn close(&self) {
    self.timer.close();
    self.spawner.close();
  }

  pub fn eager_buffering<T, R, F>(
    &self,
    count: usize,
    when_buffered: F,
  ) -> impl BackgroundThread<T, R>
  where
    T: Send + UnwindSafe + 'static,
    R: Send + Clone + 'static,
    F: Fn(Vec<T>) -> R + RefUnwindSafe + Send + Sync + 'static,
  {
    EagerBufferingThread::new(
      self.spawner.clone(),
      count,
      SharedFn::new(when_buffered.to_arc()),
    )
  }

  pub fn lazy_buffering<T, R, F>(
    &self,
    max_buffering_count: usize,
    timeout: Duration,
    when_buffered: F,
  ) -> impl BackgroundThread<T, R>
  where
    T: Send + UnwindSafe + 'static,
    R: Send + Clone + 'static,
    F: Fn(Vec<T>) -> R + RefUnwindSafe + Send + Sync + 'static,
  {
    LazyBufferingThread::new(
      self.timer.clone(),
      self.spawner.clone(),
      max_buffering_count,
      timeout,
      SharedFn::new(when_buffered.to_arc()),
    )
  }

  pub fn interval<T, R, F>(&self, timeout: Duration, f: F) -> impl BackgroundThread<T, R>
  where
    T: Send + UnwindSafe + RefUnwindSafe + 'static,
    R: Send + 'static,
    F: Fn(Option<T>) -> R + Send + RefUnwindSafe + Sync + 'static,
  {
    IntervalWorkThread::new(
      self.timer.clone(),
      self.spawner.clone(),
      timeout,
      SharedFn::new(f.to_arc()),
    )
  }

  pub fn sub_pool<T, R, F>(&self, count: usize, work: F) -> SubPool<'_, T, R>
  where
    T: Send + UnwindSafe + RefUnwindSafe + 'static,
    R: Send + 'static,
    F: Fn(T) -> R + RefUnwindSafe + Send + Sync + 'static,
  {
    SubPool::new(&self.spawner, SharedFn::new(work.to_arc()), count)
  }
}
