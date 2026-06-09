use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  sync::Mutex,
  thread::{Builder, JoinHandle},
};

use crossbeam::{
  channel::{unbounded, Receiver, Sender, TryRecvError, TrySendError},
  utils::Backoff,
};

use crate::{
  error,
  utils::{ShortenedMutex, UnwrappedSender},
};

use super::{BackgroundThread, Context, SharedFn};

const fn worker_loop<T, R>(
  name: String,
  receiver: Receiver<Context<T, R>>,
  work: SharedFn<'static, T, R>,
) -> impl Fn()
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  move || {
    let backoff = Backoff::new();

    while let Ok(ctx) = receiver.recv() {
      match ctx {
        Context::Work(v, done) => done.fulfill(work.call(v)),
        Context::Dispatch(v) => {
          if let Err(err) = work.call(v) {
            error!("error occurs in thread {}: {}", &name, err);
          }
        }
        Context::Term => return,
      }

      backoff.reset();
      while !backoff.is_completed() {
        match receiver.try_recv() {
          Ok(Context::Work(v, done)) => {
            done.fulfill(work.call(v));
            backoff.reset();
          }
          Ok(Context::Dispatch(v)) => {
            if let Err(err) = work.call(v) {
              error!("error occurs in thread {}: {}", &name, err);
            }
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
  queue: Sender<Context<T, R>>,
  threads: Mutex<Vec<JoinHandle<()>>>,
}
impl<T, R> SharedWorkThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn new<S: ToString>(
    name: S,
    size: usize,
    count: usize,
    work: SharedFn<'static, T, R>,
  ) -> Self {
    let (tx, rx) = unbounded();
    let mut threads = Vec::with_capacity(count);
    let name = name.to_string();
    for _ in 0..count {
      let thread = Builder::new()
        .name(name.clone())
        .stack_size(size)
        .spawn(worker_loop(name.clone(), rx.clone(), work.clone()))
        .unwrap();

      threads.push(thread);
    }

    Self {
      queue: tx,
      threads: Mutex::new(threads),
    }
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
    let mut threads = self.threads.l();
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
