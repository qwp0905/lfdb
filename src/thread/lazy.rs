use std::{
  cell::UnsafeCell,
  mem::ManuallyDrop,
  panic::{RefUnwindSafe, UnwindSafe},
  sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
  },
  time::Duration,
};

use crate::{
  thread::SharedFn,
  utils::{DoubleBuffer, ToArc, UnsafeBorrow, UnsafeBorrowMut},
  Result,
};

use super::{
  BackgroundThread, Context, IntervalWorkThread, OneshotFulfill, Spawner, TimerThread,
};

/**
 * A background thread that buffers incoming work items and flushes them
 * when either max_buffering_count is reached or the timeout expires —
 * whichever comes first. Unlike EagerBufferingThread, it waits to
 * accumulate a full batch before flushing.
 *
 * Suitable for tasks that benefit from batching but do not require
 * burst throughput.
 */
pub struct LazyBufferingThread<T, R> {
  queue: Arc<DoubleBuffer<(T, OneshotFulfill<Result<R>>)>>,
  flush: IntervalWorkThread<(), ()>,
  counter: Arc<AtomicUsize>,
  max_buffering_count: usize,
  when_buffered: UnsafeCell<ManuallyDrop<SharedFn<'static, Vec<T>, R>>>,
  closed: AtomicBool,
}
impl<T, R> LazyBufferingThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + Clone + 'static,
{
  pub fn new(
    timer: Arc<TimerThread>,
    spawner: Arc<Spawner>,
    max_buffering_count: usize,
    timeout: Duration,
    when_buffered: SharedFn<'static, Vec<T>, R>,
  ) -> Self {
    let queue = DoubleBuffer::new().to_arc();
    let queue_c = queue.clone();
    let counter = AtomicUsize::new(0).to_arc();
    let counter_c = counter.clone();
    let when_buffered_c = when_buffered.clone();

    let work = move |_: _| {
      counter_c.store(0, Ordering::Release);
      let queue = queue_c.switch();
      let mut values = Vec::with_capacity(queue.len());
      let mut waiting = Vec::with_capacity(queue.len());
      while let Some((v, done)) = queue.pop() {
        values.push(v);
        waiting.push(done);
      }

      if values.is_empty() {
        return;
      }

      let result = when_buffered_c.call(values).map(Ok).unwrap_or_else(Err);
      waiting
        .into_iter()
        .for_each(|done: OneshotFulfill<Result<R>>| done.fulfill(result.clone()));
    };

    let flush =
      IntervalWorkThread::new(timer, spawner, timeout, SharedFn::new(work.to_arc()));

    Self {
      flush,
      max_buffering_count,
      queue,
      counter,
      when_buffered: UnsafeCell::new(ManuallyDrop::new(when_buffered)),
      closed: AtomicBool::new(false),
    }
  }
}
unsafe impl<T, R> Send for LazyBufferingThread<T, R> {}
unsafe impl<T, R> Sync for LazyBufferingThread<T, R> {}
impl<T, R> UnwindSafe for LazyBufferingThread<T, R> {}
impl<T, R> RefUnwindSafe for LazyBufferingThread<T, R> {}

impl<T, R> BackgroundThread<T, R> for LazyBufferingThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + Clone + 'static,
{
  fn register(&self, ctx: Context<T, R>) -> bool {
    if self.closed.load(Ordering::Acquire) {
      return false;
    }
    let (v, done) = match ctx {
      Context::Work(v, done) => (v, done),
      Context::Term => {
        self.flush.close();
        return true;
      }
    };

    self.queue.push((v, done));
    let current = self.counter.fetch_add(1, Ordering::Release);
    if current != self.max_buffering_count - 1 {
      return true;
    }

    self.flush.send(());
    true
  }
  fn terminate(&self) {
    if self.closed.fetch_or(true, Ordering::Release) {
      return;
    }

    let mut values = Vec::new();
    let mut waiting = Vec::new();
    let q1 = self.queue.switch();
    while let Some((v, done)) = q1.pop() {
      values.push(v);
      waiting.push(done);
    }

    let q2 = self.queue.switch();
    while let Some((v, done)) = q2.pop() {
      values.push(v);
      waiting.push(done);
    }

    if values.is_empty() {
      return;
    }

    let result = self
      .when_buffered
      .get()
      .borrow_unsafe()
      .call(values)
      .map(Ok)
      .unwrap_or_else(Err);
    waiting
      .into_iter()
      .for_each(|done: OneshotFulfill<Result<R>>| done.fulfill(result.clone()));

    unsafe { ManuallyDrop::drop(self.when_buffered.get().borrow_mut_unsafe()) };
  }
}

#[cfg(test)]
#[path = "tests/lazy.rs"]
mod tests;
