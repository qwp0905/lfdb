use std::{
  panic::{catch_unwind, AssertUnwindSafe},
  process::abort,
  sync::Arc,
  thread::{current, Builder, JoinHandle},
};

use crossbeam::atomic::AtomicCell;

use crate::utils::error;

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

/**
 * One-shot storage for a background thread join handle.
 *
 * Background thread handles expose `close` through `&self`, so the join handle
 * must be taken through interior mutability. `ThreadSlot` makes that operation
 * safe at the API level: the first caller to `close` takes the handle, and all
 * later callers observe that the thread has already been closed.
 */
pub struct ThreadSlot<T = ()>(AtomicCell<Option<JoinHandle<T>>>);
impl<T> ThreadSlot<T> {
  pub const fn new(handle: JoinHandle<T>) -> Self {
    Self(AtomicCell::new(Some(handle)))
  }

  pub fn close(&self) -> Option<JoinHandle<T>> {
    self.0.take()
  }
}

/**
 * Utilities for spawning background threads with fail-fast panic handling.
 *
 * Background runtimes are part of the engine's internal machinery. If one of
 * them panics, the engine may have violated an internal invariant and should no
 * longer be trusted. These helpers log the panic context and abort the process
 * instead of letting a worker thread silently disappear.
 */
pub trait UnwindSpawner {
  fn spawn_unwind<T, F>(self, f: F) -> JoinHandle<T>
  where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static;
}
impl UnwindSpawner for Builder {
  fn spawn_unwind<T, F>(self, f: F) -> JoinHandle<T>
  where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
  {
    self
      .spawn(|| {
        let err = match catch_unwind(AssertUnwindSafe(f)) {
          Ok(v) => return v,
          Err(err) => err,
        };

        let thread = current();
        error!("thread {:?} panicking with error: {err:?}", thread.name());
        abort();
      })
      .unwrap()
  }
}
