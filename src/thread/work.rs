use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  sync::Arc,
  thread::JoinHandle,
};

use super::{Oneshot, OneshotFulfill};
use crate::{
  error::{Error, Result},
  utils::{SafeCallable, SafeCallableMut},
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

/**
 * A panic-safe wrapper around a mutable function for single-threaded use.
 * Allows the function to maintain state between calls via FnMut.
 */
pub struct SingleFn<'a, T, R>(Box<dyn FnMut(T) -> R + RefUnwindSafe + Send + Sync + 'a>);
impl<'a, T, R> SingleFn<'a, T, R>
where
  T: Send + UnwindSafe,
  R: Send,
{
  pub fn new<F>(f: F) -> Self
  where
    F: FnMut(T) -> R + RefUnwindSafe + Send + Sync + 'a,
  {
    Self(Box::new(f))
  }

  #[inline]
  pub fn call(&mut self, v: T) -> Result<R> {
    self.0.as_mut().safe_call_mut(v).map_err(Error::panic)
  }
}

pub struct TaskHandle<R>(Oneshot<Result<R>>);
impl<R> TaskHandle<R> {
  pub fn wait(self) -> Result<R> {
    self.0.wait()?
  }
}
impl<R> From<Oneshot<Result<R>>> for TaskHandle<R> {
  fn from(v: Oneshot<Result<R>>) -> Self {
    TaskHandle(v)
  }
}

pub struct BatchTaskHandle<R>(Vec<Oneshot<Result<R>>>);
impl<R> BatchTaskHandle<R> {
  pub fn from(v: impl Iterator<Item = Oneshot<Result<R>>>) -> Self {
    Self(v.collect())
  }
  pub fn wait(self) -> Result<Vec<R>> {
    let mut results = Vec::with_capacity(self.0.len());
    for o in self.0 {
      results.push(o.wait()??);
    }
    Ok(results)
  }
}

pub struct OnceHandle<T>(JoinHandle<T>);
impl<T> OnceHandle<T> {
  pub fn wait(self) -> Result<T> {
    self.0.join().map_err(Error::panic)
  }

  #[inline]
  pub fn new(handle: JoinHandle<T>) -> Self {
    Self(handle)
  }
}
