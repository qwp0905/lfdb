use crate::background::SingleFn;

use super::{Context, ThreadSlot, UnwindSpawner};
use std::{thread::Builder, time::Duration};

use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError, Sender};

const fn worker_loop<T, R>(
  timeout: Duration,
  mut preload: SingleFn<'static, Option<T>, R>,
  mut fallback: SingleFn<'static, Option<R>, ()>,
  receiver: Receiver<Context<T, R>>,
) -> impl FnOnce()
where
  T: Send,
  R: Send,
{
  let mut preloaded = None;
  let mut arg = None;
  move || loop {
    let result = preloaded.take().unwrap_or_else(|| preload.call(arg.take()));
    match receiver.recv_timeout(timeout) {
      Ok(Context::Work(v, done)) => {
        done.fulfill(result);
        arg = Some(v);
      }
      Ok(Context::Dispatch(v)) => arg = Some(v),
      Err(RecvTimeoutError::Timeout) => {
        fallback.call(None);
        preloaded = Some(result);
      }
      Ok(Context::Term) | Err(RecvTimeoutError::Disconnected) => {
        return fallback.call(Some(result))
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
pub struct PreloadThread<T, R> {
  channel: Sender<Context<T, R>>,
  slot: ThreadSlot,
}
impl<T, R> PreloadThread<T, R> {
  pub fn new<S: ToString + Send + 'static>(
    name: S,
    size: usize,
    timeout: Duration,
    preload: SingleFn<'static, Option<T>, R>,
    fallback: SingleFn<'static, Option<R>, ()>,
  ) -> Self
  where
    T: Send + 'static,
    R: Send + 'static,
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

  pub fn register(&self, ctx: Context<T, R>) {
    self.channel.send(ctx).unwrap()
  }

  pub fn close(&self) {
    if let Some(v) = self.slot.close() {
      self.channel.send(Context::Term).unwrap();
      v.join().unwrap();
    }
  }
}
