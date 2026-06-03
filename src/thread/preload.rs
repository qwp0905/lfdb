use crate::{thread::SingleFn, utils::UnsafeBorrowMut};

use super::{BackgroundThread, Context};
use std::{
  cell::UnsafeCell,
  panic::{RefUnwindSafe, UnwindSafe},
  thread::{Builder, JoinHandle},
  time::Duration,
};

use crossbeam::channel::{unbounded, RecvTimeoutError, Sender, TrySendError};

pub struct PreloadThread<T> {
  channel: Sender<Context<(), T>>,
  thread: UnsafeCell<Option<JoinHandle<()>>>,
}
impl<T> PreloadThread<T>
where
  T: Send + UnwindSafe + 'static,
{
  pub fn new<S: ToString>(
    name: S,
    size: usize,
    timeout: Duration,
    mut preload: SingleFn<'static, (), T>,
    mut fallback: SingleFn<'static, Option<T>, ()>,
  ) -> Self {
    let (tx, rx) = unbounded();
    let handle = Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn(move || {
        let mut preloaded = None;
        loop {
          let result = preloaded.take().unwrap_or_else(|| preload.call(()));
          match rx.recv_timeout(timeout) {
            Ok(Context::Work(_, done)) => done.fulfill(result),
            Ok(Context::Dispatch(_)) | Err(RecvTimeoutError::Timeout) => {
              let _ = fallback.call(None);
              preloaded = Some(result);
            }
            Ok(Context::Term) | Err(RecvTimeoutError::Disconnected) => {
              let _ = result.and_then(|r| fallback.call(Some(r)));
              return;
            }
          }
        }
      })
      .unwrap();

    Self {
      channel: tx,
      thread: UnsafeCell::new(Some(handle)),
    }
  }
}
unsafe impl<T> Send for PreloadThread<T> {}
unsafe impl<T> Sync for PreloadThread<T> {}
impl<T> RefUnwindSafe for PreloadThread<T> {}
impl<T> UnwindSafe for PreloadThread<T> {}

impl<T> BackgroundThread<(), T> for PreloadThread<T> {
  fn register(&self, ctx: Context<(), T>) -> bool {
    if let Err(TrySendError::Disconnected(_)) = self.channel.try_send(ctx) {
      return false;
    }
    true
  }

  fn close(&self) {
    if let Some(v) = self.thread.get().borrow_mut_unsafe().take() {
      self.channel.send(Context::Term).unwrap();
      let _ = v.join();
    }
  }
}
