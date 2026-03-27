use std::{
  cell::UnsafeCell,
  panic::{RefUnwindSafe, UnwindSafe},
  thread::{Builder, JoinHandle},
  time::Duration,
};

use crate::{
  thread::SingleFn,
  utils::{UnsafeBorrowMut, UnwrappedSender},
  Error, Result,
};

use super::{BackgroundThread, Context, WorkInput};
use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError, Sender, TrySendError};

/**
 * A background thread that processes work items on demand, and also calls
 * the work function periodically with None when no item arrives within
 * the timeout — useful for recurring maintenance tasks like GC or flush.
 */
pub struct IntervalWorkThread<T, R> {
  threads: UnsafeCell<Option<JoinHandle<()>>>,
  channel: Sender<Context<T, R>>,
}
impl<T, R> IntervalWorkThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  fn build<S: ToString>(
    name: S,
    size: usize,
    timeout: Duration,
    mut work: SingleFn<'static, Option<T>, R>,
    channel: Sender<Context<T, R>>,
    receiver: Receiver<Context<T, R>>,
  ) -> Self {
    let th = Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn(move || loop {
        match receiver.recv_timeout(timeout) {
          Ok(Context::Work(v, done)) => done.fulfill(work.call(Some(v))),
          Err(RecvTimeoutError::Timeout) => {
            let _ = work.call(None);
          }
          Ok(Context::Term) | Err(RecvTimeoutError::Disconnected) => return,
        }
      })
      .unwrap();
    Self {
      threads: UnsafeCell::new(Some(th)),
      channel,
    }
  }
  pub fn new<S: ToString>(
    name: S,
    size: usize,
    timeout: Duration,
    work: SingleFn<'static, Option<T>, R>,
  ) -> Self {
    let (tx, rx) = unbounded();
    Self::build(name, size, timeout, work, tx, rx)
  }

  pub fn from_channel<S: ToString>(
    name: S,
    size: usize,
    timeout: Duration,
    input: WorkInput<T, R>,
    work: SingleFn<'static, Option<T>, R>,
  ) -> Result<Self> {
    let (tx, rx) = match input.take() {
      Some(rx) => rx,
      None => return Err(Error::ThreadConflict),
    };
    Ok(Self::build(name, size, timeout, work, tx, rx))
  }
}
unsafe impl<T, R> Send for IntervalWorkThread<T, R> {}
unsafe impl<T, R> Sync for IntervalWorkThread<T, R> {}
impl<T, R> RefUnwindSafe for IntervalWorkThread<T, R> {}
impl<T, R> UnwindSafe for IntervalWorkThread<T, R> {}
impl<T, R> BackgroundThread<T, R> for IntervalWorkThread<T, R> {
  fn register(&self, ctx: Context<T, R>) -> bool {
    match self.channel.try_send(ctx) {
      Err(TrySendError::Disconnected(_)) => false,
      _ => true,
    }
  }

  fn close(&self) {
    if let Some(v) = self.threads.get().borrow_mut_unsafe().take() {
      self.channel.must_send(Context::Term);
      let _ = v.join();
    }
  }
}

#[cfg(test)]
#[path = "tests/interval.rs"]
mod tests;
