use super::{oneshot, Context, EventBindings, Oneshot, OwnedSubscription};

/**
 * A trait for background threads that accept work items and return results.
 */
pub trait BackgroundThread<T, R = ()>: Send + Sync {
  fn register(&self, ctx: Context<T, R>);
  fn close(&self);

  #[inline]
  fn execute(&self, v: T) -> Oneshot<R> {
    let (done_r, done_t) = oneshot();
    self.register(Context::Work(v, done_t));
    done_r
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
