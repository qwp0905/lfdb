use std::{
  panic::{catch_unwind, AssertUnwindSafe},
  process::abort,
  thread::{current, Builder, JoinHandle},
};

use crate::error;

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
