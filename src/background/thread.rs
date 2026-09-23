use super::Oneshot;

/**
 * Stop the runtime and join its worker thread(s).
 *
 * `close` is the synchronization boundary for background runtimes. After it
 * is called, the runtime stops accepting requests and the caller waits for
 * the underlying thread(s) to terminate. Implementations use this point to
 * join the worker thread(s), which also makes background panics observable by
 * the caller.
 */
pub trait Close: Send + Sync {
  fn close(&self);
  /**
   * Automatically close at drop.
   */
  fn into_once(self) -> OnceThread<Self>
  where
    Self: Sized,
  {
    OnceThread(self)
  }
}
pub struct OnceThread<T: Close>(T);
impl<T: Close> Drop for OnceThread<T> {
  fn drop(&mut self) {
    Close::close(&self.0);
  }
}
impl<T: Close> std::ops::Deref for OnceThread<T> {
  type Target = T;
  fn deref(&self) -> &Self::Target {
    &self.0
  }
}
/**
 * Submit a command and return a completion handle.
 *
 * `execute` is used when the caller needs a response from the background
 * runtime. The work item is wrapped in a `Context::Work` together with a
 * oneshot fulfiller, and the returned `Oneshot` can be waited on by the
 * caller.
 */
pub trait Execute<T, R>: Close {
  fn execute(&self, value: T) -> Oneshot<R>;
}
