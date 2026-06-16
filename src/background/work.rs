use std::sync::Arc;

use super::OneshotFulfill;

pub enum Context<T, R> {
  Work(T, OneshotFulfill<R>),
  Dispatch(T),
  Term,
}

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

pub struct SingleFn<'a, T, R>(Box<dyn FnMut(T) -> R + Send + Sync + 'a>);
impl<'a, T, R> SingleFn<'a, T, R>
where
  T: Send,
  R: Send,
{
  pub fn new<F>(f: F) -> Self
  where
    F: FnMut(T) -> R + Send + Sync + 'a,
  {
    Self(Box::new(f))
  }

  #[inline]
  pub fn call(&mut self, v: T) -> R {
    self.0(v)
  }
}
