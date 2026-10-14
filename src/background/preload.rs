use std::{
  sync::Arc,
  thread::{park, Builder, Thread},
};

use crate::utils::MpscQueue;

use super::{oneshot, OneshotFulfill, SingleFn, ThreadSlot, UnwindSpawner};

enum Context<T, R> {
  Work(T, OneshotFulfill<R>),
  Term,
}

const fn worker_loop<T>(
  mut preload: SingleFn<'static, (), T>,
  mut fallback: SingleFn<'static, T, ()>,
  queue: Arc<MpscQueue<Context<(), T>>>,
) -> impl FnOnce()
where
  T: Send,
{
  let mut preloaded = None;
  move || loop {
    let loaded = preloaded.take().unwrap_or_else(|| preload.call(()));
    if let Some(ctx) = unsafe { queue.pop() } {
      match ctx {
        Context::Work(_, done) => done.fulfill(loaded),
        Context::Term => return fallback.call(loaded),
      };
      continue;
    };

    preloaded = Some(loaded);
    park();
  }
}

/**
 * Single-worker runtime that keeps one value precomputed.
 *
 * This is one of the single-threaded runtime variants. It packages a specific
 * usage pattern: keep one value prepared ahead of demand and return it when a
 * request arrives.
 */
pub struct PreloadThread<T: Send> {
  queue: Arc<MpscQueue<Context<(), T>>>,
  waker: Thread,
  slot: ThreadSlot,
}
impl<T: Send> PreloadThread<T> {
  pub fn new<S: ToString + Send + 'static>(
    name: S,
    size: usize,
    preload: SingleFn<'static, (), T>,
    fallback: SingleFn<'static, T, ()>,
  ) -> Self
  where
    T: Send + 'static,
  {
    let queue = Arc::new(MpscQueue::new());
    let handle = Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn_unwind(worker_loop(preload, fallback, queue.clone()));
    let waker = handle.thread().clone();
    Self {
      queue,
      waker,
      slot: ThreadSlot::new(handle),
    }
  }

  fn register(&self, ctx: Context<(), T>) {
    self.queue.push(ctx);
    self.waker.unpark();
  }

  pub fn execute(&self, _: ()) -> super::Oneshot<T> {
    let (o, f) = oneshot();
    self.register(Context::Work((), f));
    o
  }

  pub fn close(&self) {
    if let Some(handle) = self.slot.close() {
      self.queue.push(Context::Term);
      handle.thread().unpark();
      handle.join().unwrap();
    }
  }
}

impl<T: Send> Drop for PreloadThread<T> {
  fn drop(&mut self) {
    self.close();
  }
}
