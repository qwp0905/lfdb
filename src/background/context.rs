use std::{cell::UnsafeCell, sync::Arc};

use super::OneshotFulfill;

/**
 * Message protocol consumed by background runtimes.
 *
 * `Work` is a command that must send a result back through the supplied
 * oneshot fulfiller. `Dispatch` is a fire-and-forget event with no response
 * channel. `Term` asks the runtime to stop and is used by `close`.
 */
pub enum ExecutableContext<T, R> {
  Work(T, OneshotFulfill<R>),
  Dispatch(T),
  Term,
}
pub enum ExecuteOnlyContext<T, R> {
  Work(T, OneshotFulfill<R>),
  Term,
}
/**
 * Small utility types shared by background runtime implementations.
 *
 * This module defines the message protocol sent to background runtimes and the
 * handler wrappers used to move user-provided functions into worker threads.
 */
pub struct SharedFn<'a, T, R>(Arc<dyn Fn(T) -> R + Send + Sync + 'a>);
impl<'a, T, R> SharedFn<'a, T, R> {
  pub fn new(f: impl Fn(T) -> R + Send + Sync + 'a) -> Self {
    Self(Arc::new(f))
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
impl<'a, T, R> SingleFn<'a, T, R> {
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

pub struct UnsafeFn<T, R>(Arc<UnsafeCell<dyn FnMut(T) -> R + Send>>);
impl<T, R> UnsafeFn<T, R> {
  pub fn new<F: FnMut(T) -> R + Send + 'static>(handler: F) -> Self {
    Self(Arc::new(UnsafeCell::new(handler)))
  }
  pub unsafe fn call(&self, arg: T) -> R {
    (*self.0.get())(arg)
  }
}
impl<T, R> Clone for UnsafeFn<T, R> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}
unsafe impl<T: Send, R: Send> Send for UnsafeFn<T, R> {}
unsafe impl<T: Send, R: Send> Sync for UnsafeFn<T, R> {}
