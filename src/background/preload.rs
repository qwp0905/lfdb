use crate::background::SingleFn;

use super::{oneshot, Close, Execute, ExecuteOnlyContext, ThreadSlot, UnwindSpawner};
use std::{thread::Builder, time::Duration};

use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError, Sender};

pub enum Fallback<T> {
  Timeout,
  Terminated(T),
}

const fn worker_loop<T>(
  timeout: Duration,
  mut preload: SingleFn<'static, (), T>,
  mut fallback: SingleFn<'static, Fallback<T>, ()>,
  receiver: Receiver<ExecuteOnlyContext<(), T>>,
) -> impl FnOnce()
where
  T: Send,
{
  let mut preloaded = None;
  move || loop {
    let result = preloaded.take().unwrap_or_else(|| preload.call(()));
    match receiver.recv_timeout(timeout) {
      Ok(ExecuteOnlyContext::Work(_, done)) => done.fulfill(result),
      Err(RecvTimeoutError::Timeout) => {
        fallback.call(Fallback::Timeout);
        preloaded = Some(result);
      }
      Ok(ExecuteOnlyContext::Term) | Err(RecvTimeoutError::Disconnected) => {
        return fallback.call(Fallback::Terminated(result))
      }
    }
  }
}

/**
 * Single-worker runtime that keeps one value precomputed.
 *
 * This is one of the single-threaded runtime variants. It packages a specific
 * usage pattern: keep one value prepared ahead of demand and return it when a
 * request arrives.
 *
 * The worker creates a value with `preload` before it is requested. When a
 * `Work` message arrives, the preloaded value is returned immediately through
 * the oneshot fulfiller. If no request arrives before the timeout, the value is
 * kept for the next wait cycle after reporting the timeout through
 * `fallback(None)`.
 *
 * On shutdown, any unused preloaded value is passed to `fallback(Some(value))`
 * so the caller can clean it up or return it to another owner.
 */
pub struct PreloadThread<T> {
  channel: Sender<ExecuteOnlyContext<(), T>>,
  slot: ThreadSlot,
}
impl<T> PreloadThread<T> {
  pub fn new<S: ToString + Send + 'static>(
    name: S,
    size: usize,
    timeout: Duration,
    preload: SingleFn<'static, (), T>,
    fallback: SingleFn<'static, Fallback<T>, ()>,
  ) -> Self
  where
    T: Send + 'static,
  {
    let (tx, rx) = unbounded();
    let handle = Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn_unwind(worker_loop(timeout, preload, fallback, rx));

    Self {
      channel: tx,
      slot: ThreadSlot::new(handle),
    }
  }

  fn register(&self, ctx: ExecuteOnlyContext<(), T>) {
    self.channel.send(ctx).unwrap()
  }
}
impl<T: Send> Close for PreloadThread<T> {
  fn close(&self) {
    if let Some(v) = self.slot.close() {
      self.channel.send(ExecuteOnlyContext::Term).unwrap();
      v.join().unwrap();
    }
  }
}
impl<T: Send> Execute<(), T> for PreloadThread<T> {
  fn execute(&self, _: ()) -> super::Oneshot<T> {
    let (o, f) = oneshot();
    self.register(ExecuteOnlyContext::Work((), f));
    o
  }
}
