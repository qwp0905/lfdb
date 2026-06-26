use std::{sync::Arc, thread::JoinHandle};

use super::OneshotFulfill;

/**
 * Message protocol consumed by background runtimes.
 *
 * `Work` is a command that must send a result back through the supplied
 * oneshot fulfiller. `Dispatch` is a fire-and-forget event with no response
 * channel. `Term` asks the runtime to stop and is used by `close`.
 */
pub enum Context<T, R> {
  Work(T, OneshotFulfill<R>),
  Dispatch(T),
  Term,
}
/**
 * Small utility types shared by background runtime implementations.
 *
 * This module defines the message protocol sent to background runtimes and the
 * handler wrappers used to move user-provided functions into worker threads.
 */
pub struct SharedFn<'a, T, R>(Arc<dyn Fn(T) -> R + Send + Sync + 'a>);
impl<'a, T, R> SharedFn<'a, T, R>
where
  T: Send + 'a,
  R: Send + 'a,
{
  pub const fn new(f: Arc<dyn Fn(T) -> R + Send + Sync + 'a>) -> Self {
    Self(f)
  }
  #[inline]
  pub fn call(&self, v: T) -> R {
    self.0(v)
  }
}
impl<'a, T, R> Clone for SharedFn<'a, T, R> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

pub struct SingleFn<'a, T, R>(Box<dyn FnMut(T) -> R + Send + 'a>);
impl<'a, T, R> SingleFn<'a, T, R>
where
  T: Send,
  R: Send,
{
  pub fn new<F>(f: F) -> Self
  where
    F: FnMut(T) -> R + Send + 'a,
  {
    Self(Box::new(f))
  }

  #[inline]
  pub fn call(&mut self, v: T) -> R {
    self.0(v)
  }
}

/**
 * Join handle for a one-shot background task.
 *
 * `wait` deliberately unwraps the thread join result. A panic in a background
 * task means the engine has reached an invalid internal state; the caller
 * should observe that panic instead of treating it as a recoverable error.
 */
pub struct OnceHandle<T>(JoinHandle<T>);
impl<T> OnceHandle<T> {
  pub fn wait(self) -> T {
    self.0.join().unwrap()
  }

  #[inline]
  pub const fn new(handle: JoinHandle<T>) -> Self {
    Self(handle)
  }
}
