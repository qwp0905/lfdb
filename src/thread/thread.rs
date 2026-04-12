use std::panic::{RefUnwindSafe, UnwindSafe};

use crate::Error;

use super::{oneshot, BatchTaskHandle, Context, TaskHandle};

/**
 * A trait for background threads that accept work items and return results.
 * send() returns a WorkResult which resolves to an error if the worker
 * thread is closed or if the work panicked.
 */
pub trait BackgroundThread<T, R = ()>: Send + Sync + RefUnwindSafe + UnwindSafe {
  /**
   * return flag of success or failed to register work to thread.
   */
  fn register(&self, ctx: Context<T, R>) -> bool;
  fn close(&self);

  #[inline]
  fn execute(&self, v: T) -> TaskHandle<R> {
    let (done_r, done_t) = oneshot();
    if self.register(Context::Work(v, done_t)) {
      return TaskHandle::from(done_r);
    }

    drop(done_r);
    let (done_r, done_t) = oneshot();
    done_t.fulfill(Err(Error::WorkerClosed));
    TaskHandle::from(done_r)
  }

  fn dispatch(&self, v: T) {
    self.register(Context::Dispatch(v));
  }

  fn execute_batch(&self, v: Vec<T>) -> BatchTaskHandle<R> {
    BatchTaskHandle::from(v.into_iter().map(|i| {
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
