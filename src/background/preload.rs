use crate::{background::SingleFn, error};

use super::{BackgroundThread, Context, ThreadSlot};
use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  thread::Builder,
  time::Duration,
};

use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError, Sender, TrySendError};

const fn worker_loop<T>(
  timeout: Duration,
  mut preload: SingleFn<'static, (), T>,
  mut fallback: SingleFn<'static, Option<T>, ()>,
  name: String,
  receiver: Receiver<Context<(), T>>,
) -> impl FnOnce()
where
  T: Send + UnwindSafe,
{
  let mut preloaded = None;
  move || loop {
    let result = preloaded.take().unwrap_or_else(|| preload.call(()));
    match receiver.recv_timeout(timeout) {
      Ok(Context::Work(_, done)) => done.fulfill(result),
      Ok(Context::Dispatch(_)) | Err(RecvTimeoutError::Timeout) => {
        if let Err(err) = fallback.call(None) {
          error!("error occurs in thread {}: {}", name, err);
        }
        preloaded = Some(result);
      }
      Ok(Context::Term) | Err(RecvTimeoutError::Disconnected) => {
        if let Err(err) = result.and_then(|r| fallback.call(Some(r))) {
          error!("error occurs in thread {}: {}", name, err);
        }
        return;
      }
    }
  }
}

pub struct PreloadThread<T> {
  channel: Sender<Context<(), T>>,
  slot: ThreadSlot,
}
impl<T> PreloadThread<T>
where
  T: Send + UnwindSafe + 'static,
{
  pub fn new<S: ToString + Send + 'static>(
    name: S,
    size: usize,
    timeout: Duration,
    preload: SingleFn<'static, (), T>,
    fallback: SingleFn<'static, Option<T>, ()>,
  ) -> Self {
    let (tx, rx) = unbounded();
    let handle = Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn(worker_loop(
        timeout,
        preload,
        fallback,
        name.to_string(),
        rx,
      ))
      .unwrap();

    Self {
      channel: tx,
      slot: ThreadSlot::new(handle),
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
    if let Some(v) = self.slot.close() {
      self.channel.send(Context::Term).unwrap();
      let _ = v.join();
    }
  }
}
