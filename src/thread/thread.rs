use std::panic::{RefUnwindSafe, UnwindSafe};

use crossbeam::channel::{unbounded, Receiver, Sender, TrySendError};

use crate::Error;

use super::{oneshot, BatchWorkResult, Context, WorkResult};

pub trait BackgroundThread<T, R = ()>: Send + Sync + RefUnwindSafe + UnwindSafe {
  /**
   * return flag of success or failed to register work to thread.
   */
  fn register(&self, ctx: Context<T, R>) -> bool;
  fn close(&self);

  #[inline]
  fn send(&self, v: T) -> WorkResult<R> {
    let (done_r, done_t) = oneshot();
    if self.register(Context::Work(v, done_t)) {
      return WorkResult::from(done_r);
    }

    drop(done_r);
    let (done_r, done_t) = oneshot();
    done_t.fulfill(Err(Error::WorkerClosed));
    WorkResult::from(done_r)
  }

  fn send_batch(&self, v: Vec<T>) -> BatchWorkResult<R> {
    BatchWorkResult::from(v.into_iter().map(|i| {
      let (done_r, done_t) = oneshot();
      if self.register(Context::Work(i, done_t)) {
        return done_r;
      }
      drop(done_r);

      let (done_r, done_t) = oneshot();
      done_t.fulfill(Err(Error::WorkerClosed));
      done_r
    }))
  }
}

pub struct WorkInput<T, R> {
  sender: Sender<Context<T, R>>,
  receiver: Option<Receiver<Context<T, R>>>,
}
impl<T, R> WorkInput<T, R> {
  pub fn new() -> Self {
    let (sender, receiver) = unbounded();
    Self {
      sender,
      receiver: Some(receiver),
    }
  }
  pub fn send(&self, v: T) -> WorkResult<R> {
    let (done_r, done_t) = oneshot();
    if let Err(TrySendError::Disconnected(_)) =
      self.sender.try_send(Context::Work(v, done_t))
    {
      drop(done_r);
      let (done_r, done_t) = oneshot();
      done_t.fulfill(Err(Error::WorkerClosed));
      return WorkResult::from(done_r);
    }
    WorkResult::from(done_r)
  }
  pub fn copy(&self) -> Self {
    Self {
      sender: self.sender.clone(),
      receiver: None,
    }
  }
  pub fn take(mut self) -> Option<(Sender<Context<T, R>>, Receiver<Context<T, R>>)> {
    let rx = self.receiver.take()?;
    Some((self.sender, rx))
  }
}
