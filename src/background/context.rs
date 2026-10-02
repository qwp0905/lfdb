use std::sync::Arc;

use super::OneshotFulfill;

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
