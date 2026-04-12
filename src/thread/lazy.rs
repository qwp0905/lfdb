use std::{
  cell::UnsafeCell,
  panic::{RefUnwindSafe, UnwindSafe},
  thread::{Builder, JoinHandle},
  time::Duration,
};

use crate::{
  utils::{AsTimer, UnsafeBorrowMut, UnwrappedSender},
  Result,
};

use super::{BackgroundThread, Context, OneshotFulfill, SingleFn};
use crossbeam::channel::{unbounded, RecvTimeoutError, Sender, TrySendError};

/**
 * A background thread that buffers incoming work items and flushes them
 * when either max_buffering_count is reached or the timeout expires —
 * whichever comes first. Unlike EagerBufferingThread, it waits to
 * accumulate a full batch before flushing.
 *
 * Suitable for tasks that benefit from batching but do not require
 * burst throughput.
 */
pub struct LazyBufferingThread<T, R> {
  threads: UnsafeCell<Option<JoinHandle<()>>>,
  channel: Sender<Context<T, R>>,
}
impl<T, R> LazyBufferingThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + Clone + 'static,
{
  pub fn new<S: ToString>(
    name: S,
    size: usize,
    max_buffering_count: usize,
    timeout: Duration,
    mut when_buffered: SingleFn<'static, Vec<T>, R>,
  ) -> Self {
    let (tx, rx) = unbounded();

    let th = Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn(move || {
        let mut buffered = Vec::with_capacity(max_buffering_count);
        let mut timer = timeout.as_timer();

        let mut flush = |buffer: &mut Vec<(T, Option<OneshotFulfill<Result<R>>>)>| {
          if buffer.is_empty() {
            return;
          }

          let (values, waiting): (Vec<_>, Vec<_>) = buffer.drain(..).unzip();
          let result = when_buffered.call(values).map(Ok).unwrap_or_else(Err);
          waiting
            .into_iter()
            .flatten()
            .for_each(|done| done.fulfill(result.clone()));
        };

        loop {
          match rx.recv_timeout(timer.get_remain()) {
            Ok(Context::Work(v, done)) => {
              buffered.push((v, Some(done)));
              if buffered.len() < max_buffering_count {
                timer.check();
                continue;
              }
            }
            Ok(Context::Dispatch(v)) => {
              buffered.push((v, None));
              if buffered.len() < max_buffering_count {
                timer.check();
                continue;
              }
            }
            Ok(Context::Term) | Err(RecvTimeoutError::Disconnected) => {
              return flush(&mut buffered)
            }
            Err(RecvTimeoutError::Timeout) => {}
          }

          flush(&mut buffered);
          timer.reset();
        }
      })
      .unwrap();
    Self {
      threads: UnsafeCell::new(Some(th)),
      channel: tx,
    }
  }
}
unsafe impl<T, R> Send for LazyBufferingThread<T, R> {}
unsafe impl<T, R> Sync for LazyBufferingThread<T, R> {}
impl<T, R> UnwindSafe for LazyBufferingThread<T, R> {}
impl<T, R> RefUnwindSafe for LazyBufferingThread<T, R> {}
impl<T, R> BackgroundThread<T, R> for LazyBufferingThread<T, R> {
  fn register(&self, ctx: Context<T, R>) -> bool {
    match self.channel.try_send(ctx) {
      Err(TrySendError::Disconnected(_)) => false,
      _ => true,
    }
  }
  fn close(&self) {
    if let Some(v) = self.threads.get().borrow_mut_unsafe().take() {
      self.channel.must_send(Context::Term);
      let _ = v.join();
    }
  }
}
