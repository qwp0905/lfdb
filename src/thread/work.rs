use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  sync::Arc,
};

use super::OneshotFulfill;
use crate::{
  error::{Error, Result},
  utils::SafeCallable,
};

pub enum Context<T, R> {
  Work(T, OneshotFulfill<Result<R>>),
  Term,
}

/**
 * A panic-safe wrapper around a shared, immutable function.
 * Can be cloned and called concurrently across threads.
 */
pub struct SharedFn<'a, T, R>(Arc<dyn Fn(T) -> R + RefUnwindSafe + Send + Sync + 'a>);
impl<'a, T, R> SharedFn<'a, T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn new(f: Arc<dyn Fn(T) -> R + RefUnwindSafe + Send + Sync + 'a>) -> Self {
    Self(f)
  }
  #[inline]
  pub fn call(&self, v: T) -> Result<R> {
    self.0.as_ref().safe_call(v).map_err(Error::panic)
  }
}
impl<'a, T, R> Clone for SharedFn<'a, T, R> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

// /**
//  * A panic-safe wrapper around a mutable function for single-threaded use.
//  * Allows the function to maintain state between calls via FnMut.
//  */
// pub struct SingleFn<'a, T, R>(Box<dyn FnMut(T) -> R + RefUnwindSafe + Send + Sync + 'a>);
// impl<'a, T, R> SingleFn<'a, T, R>
// where
//   T: Send + UnwindSafe,
//   R: Send,
// {
//   pub fn new<F>(f: F) -> Self
//   where
//     F: FnMut(T) -> R + RefUnwindSafe + Send + Sync + 'a,
//   {
//     Self(Box::new(f))
//   }

//   #[inline]
//   pub fn call(&mut self, v: T) -> Result<R> {
//     self.0.as_mut().safe_call_mut(v).map_err(Error::panic)
//   }
// }
