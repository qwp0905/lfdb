use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  thread::Builder,
  time::Duration,
};

use crate::error;

use super::{BackgroundThread, Context, SingleFn, ThreadSlot};
use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError, Sender, TrySendError};

const fn worker_loop<T, R>(
  receiver: Receiver<Context<T, R>>,
  mut work: SingleFn<'static, Option<T>, R>,
  timeout: Duration,
  name: String,
) -> impl FnOnce()
where
  T: Send + UnwindSafe,
  R: Send,
{
  move || loop {
    match receiver.recv_timeout(timeout) {
      Ok(Context::Work(v, done)) => done.fulfill(work.call(Some(v))),
      Ok(Context::Dispatch(v)) => {
        if let Err(err) = work.call(Some(v)) {
          error!("error occurs in thread {}: {}", name, err);
        }
      }
      Err(RecvTimeoutError::Timeout) => {
        if let Err(err) = work.call(None) {
          error!("error occurs in thread {}: {}", name, err);
        }
      }
      Ok(Context::Term) | Err(RecvTimeoutError::Disconnected) => return,
    }
  }
}

/**
 * A background thread that processes work items on demand, and also calls
 * the work function periodically with None when no item arrives within
 * the timeout — useful for recurring maintenance tasks like GC or flush.
 */
pub struct IntervalWorkThread<T, R> {
  channel: Sender<Context<T, R>>,
  slot: ThreadSlot,
}
impl<T, R> IntervalWorkThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn new<S: ToString + Send + 'static>(
    name: S,
    size: usize,
    timeout: Duration,
    work: SingleFn<'static, Option<T>, R>,
  ) -> Self {
    let (channel, receiver) = unbounded();
    let handle = Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn(worker_loop(receiver, work, timeout, name.to_string()))
      .unwrap();
    Self {
      channel,
      slot: ThreadSlot::new(handle),
    }
  }
}
unsafe impl<T, R> Send for IntervalWorkThread<T, R> {}
unsafe impl<T, R> Sync for IntervalWorkThread<T, R> {}
impl<T, R> RefUnwindSafe for IntervalWorkThread<T, R> {}
impl<T, R> UnwindSafe for IntervalWorkThread<T, R> {}
impl<T, R> BackgroundThread<T, R> for IntervalWorkThread<T, R> {
  fn register(&self, ctx: Context<T, R>) -> bool {
    !matches!(
      self.channel.try_send(ctx),
      Err(TrySendError::Disconnected(_))
    )
  }

  fn close(&self) {
    if let Some(v) = self.slot.close() {
      self.channel.send(Context::Term).unwrap();
      let _ = v.join();
    }
  }
}

#[cfg(test)]
#[path = "tests/interval.rs"]
mod tests;
