use std::{
  cell::UnsafeCell,
  panic::{RefUnwindSafe, UnwindSafe},
  sync::Arc,
  thread::{Builder, JoinHandle},
};

use crossbeam::{
  channel::{unbounded, Receiver, Sender, TryRecvError, TrySendError},
  utils::Backoff,
};

use crate::{
  error::Result,
  utils::{ToArc, UnsafeBorrowMut, UnwrappedSender},
};

use super::{BackgroundThread, Context, SharedFn};

fn worker_loop<T, R>(
  receiver: Receiver<Context<T, R>>,
  work: SharedFn<'static, T, R>,
) -> impl Fn()
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  move || {
    let backoff = Backoff::new();

    while let Ok(Context::Work(v, done)) = receiver.recv() {
      done.fulfill(work.call(v));

      backoff.reset();
      while !backoff.is_completed() {
        match receiver.try_recv() {
          Ok(Context::Work(v, done)) => {
            done.fulfill(work.call(v));
            backoff.reset();
          }
          Ok(Context::Term) | Err(TryRecvError::Disconnected) => return,
          Err(TryRecvError::Empty) => backoff.snooze(),
        }
      }
    }
  }
}

/**
 * Multiple worker threads sharing a single channel for task distribution.
 * Suitable for tasks that require burst throughput but have long idle periods.
 */
pub struct SharedWorkThread<T, R = ()> {
  threads: UnsafeCell<Vec<JoinHandle<()>>>,
  queue: Sender<Context<T, R>>,
}
impl<T, R> SharedWorkThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  fn build__<S: ToString, F, E, W>(
    name: S,
    size: usize,
    count: usize,
    build: F,
  ) -> std::result::Result<Self, E>
  where
    F: Fn(usize) -> std::result::Result<Arc<W>, E>,
    W: Fn(T) -> R + RefUnwindSafe + Send + Sync + 'static,
  {
    let (tx, rx) = unbounded();
    let mut threads = Vec::with_capacity(count);
    for i in 0..count {
      let thread = Builder::new()
        .name(format!("{}-{}", name.to_string(), i))
        .stack_size(size)
        .spawn(worker_loop(rx.clone(), SharedFn::new(build(i)?)))
        .unwrap();

      threads.push(thread);
    }

    Ok(Self {
      queue: tx,
      threads: UnsafeCell::new(threads),
    })
  }
  pub fn new<S: ToString, F>(name: S, size: usize, count: usize, build: F) -> Self
  where
    F: Fn(T) -> R + RefUnwindSafe + Send + Sync + 'static,
  {
    let build = build.to_arc();
    Self::build__(name, size, count, |_| Ok(build.clone()) as Result<Arc<F>>).unwrap()
  }
}

unsafe impl<T, R> Send for SharedWorkThread<T, R> {}
unsafe impl<T, R> Sync for SharedWorkThread<T, R> {}
impl<T, R> RefUnwindSafe for SharedWorkThread<T, R> {}
impl<T, R> UnwindSafe for SharedWorkThread<T, R> {}

impl<T, R> BackgroundThread<T, R> for SharedWorkThread<T, R> {
  fn register(&self, ctx: Context<T, R>) -> bool {
    match self.queue.try_send(ctx) {
      Err(TrySendError::Disconnected(_)) => false,
      _ => true,
    }
  }
  fn close(&self) {
    let threads = self.threads.get().borrow_mut_unsafe();
    for _ in 0..threads.len() {
      self.queue.must_send(Context::Term);
    }
    for th in threads.drain(..) {
      let _ = th.join();
    }
  }
}

#[cfg(test)]
#[path = "tests/shared.rs"]
mod tests;
