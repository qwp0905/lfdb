use std::panic::{RefUnwindSafe, UnwindSafe};

use crate::Error;

use super::{oneshot, Context, EventBindings, OwnedSubscription, TaskHandle};

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
      return TaskHandle::new(done_r);
    }

    TaskHandle::fulfilled(Err(Error::WorkerClosed))
  }

  fn dispatch(&self, v: T) {
    self.register(Context::Dispatch(v));
  }
}
impl<T, R> OwnedSubscription<T> for dyn BackgroundThread<T, R> {
  fn handle(&self, event: T) {
    self.dispatch(event);
  }
}
impl<T, R> EventBindings for dyn BackgroundThread<T, R>
where
  T: Send + Sync + 'static,
  R: 'static,
{
  type Owned = (T, ());

  type Shared = ();
}
