use std::{thread::Builder, time::Duration};

use super::{
  oneshot, Close, Dispatch, ExecutableContext, Execute, SingleFn, ThreadSlot,
  UnwindSpawner,
};
use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError, Sender};

const fn worker_loop<T, R>(
  receiver: Receiver<ExecutableContext<T, R>>,
  mut work: SingleFn<'static, Option<T>, R>,
  timeout: Duration,
) -> impl FnOnce()
where
  T: Send,
  R: Send,
{
  move || loop {
    match receiver.recv_timeout(timeout) {
      Ok(ExecutableContext::Work(v, done)) => done.fulfill(work.call(Some(v))),
      Ok(ExecutableContext::Dispatch(v)) => {
        let _ = work.call(Some(v));
      }
      Err(RecvTimeoutError::Timeout) => {
        let _ = work.call(None);
      }
      Ok(ExecutableContext::Term) | Err(RecvTimeoutError::Disconnected) => return,
    }
  }
}

/**
 * Single-worker runtime with idle-time ticks.
 *
 * Explicit submissions are delivered to the handler as `Some(T)`. If no
 * submission arrives before the timeout, the handler is called with `None`,
 * which represents an idle tick.
 *
 * The timeout is not a precise periodic schedule. A tick means "no message has
 * arrived for at least this duration", so continuous explicit work can delay
 * ticks. This makes the runtime suitable for maintenance work that should run
 * during idle gaps.
 *
 * This is a single-threaded runtime: it uses `SingleFn`, so explicit work and
 * idle ticks are serialized through one worker thread.
 */
pub struct IntervalWorkThread<T, R = ()> {
  channel: Sender<ExecutableContext<T, R>>,
  slot: ThreadSlot,
}
impl<T, R> IntervalWorkThread<T, R> {
  pub fn new<S: ToString + Send + 'static>(
    name: S,
    size: usize,
    timeout: Duration,
    work: SingleFn<'static, Option<T>, R>,
  ) -> Self
  where
    T: Send + 'static,
    R: Send + 'static,
  {
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

  fn register(&self, ctx: ExecutableContext<T, R>) {
    self.channel.send(ctx).unwrap()
  }
}

impl<T: Send, R: Send> Close for IntervalWorkThread<T, R> {
  fn close(&self) {
    if let Some(v) = self.slot.close() {
      self.channel.send(ExecutableContext::Term).unwrap();
      v.join().unwrap();
    }
  }
}
impl<T: Send, R: Send> Dispatch<T> for IntervalWorkThread<T, R> {
  fn dispatch(&self, value: T) {
    self.register(ExecutableContext::Dispatch(value));
  }
}
impl<T: Send, R: Send> Execute<T, R> for IntervalWorkThread<T, R> {
  fn execute(&self, value: T) -> super::Oneshot<R> {
    let (o, f) = oneshot();
    self.register(ExecutableContext::Work(value, f));
    o
  }
}

#[cfg(test)]
#[path = "tests/interval.rs"]
mod tests;
