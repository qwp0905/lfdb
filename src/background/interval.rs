use std::{thread::Builder, time::Duration};

use super::{BackgroundThread, Context, SingleFn, ThreadSlot, UnwindSpawner};
use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError, Sender};

const fn worker_loop<T, R>(
  receiver: Receiver<Context<T, R>>,
  mut work: SingleFn<'static, Option<T>, R>,
  timeout: Duration,
) -> impl FnOnce()
where
  T: Send,
  R: Send,
{
  move || loop {
    match receiver.recv_timeout(timeout) {
      Ok(Context::Work(v, done)) => done.fulfill(work.call(Some(v))),
      Ok(Context::Dispatch(v)) => {
        let _ = work.call(Some(v));
      }
      Err(RecvTimeoutError::Timeout) => {
        let _ = work.call(None);
      }
      Ok(Context::Term) | Err(RecvTimeoutError::Disconnected) => return,
    }
  }
}

/**
 * A background thread that processes work items on demand, and also calls
 * the work function periodically with None when no item arrives within
 * the timeout — useful for recurring maintenance tasks like GC or flush.
 */
pub struct IntervalWorkThread<T, R> {
  channel: Sender<Context<T, R>>,
  slot: ThreadSlot,
}
impl<T, R> IntervalWorkThread<T, R>
where
  T: Send + 'static,
  R: Send + 'static,
{
  pub fn new<S: ToString + Send + 'static>(
    name: S,
    size: usize,
    timeout: Duration,
    work: SingleFn<'static, Option<T>, R>,
  ) -> Self {
    let (channel, receiver) = unbounded();
    let handle = Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn_unwind(worker_loop(receiver, work, timeout));
    Self {
      channel,
      slot: ThreadSlot::new(handle),
    }
  }
}
unsafe impl<T, R> Send for IntervalWorkThread<T, R> {}
unsafe impl<T, R> Sync for IntervalWorkThread<T, R> {}
impl<T, R> BackgroundThread<T, R> for IntervalWorkThread<T, R> {
  fn register(&self, ctx: Context<T, R>) {
    self.channel.send(ctx).unwrap()
  }

  fn close(&self) {
    if let Some(v) = self.slot.close() {
      let _ = self.channel.send(Context::Term);
      v.join().unwrap();
    }
  }
}

#[cfg(test)]
#[path = "tests/interval.rs"]
mod tests;
