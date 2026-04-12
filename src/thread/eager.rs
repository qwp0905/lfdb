use std::{
  cell::UnsafeCell,
  panic::{RefUnwindSafe, UnwindSafe},
  sync::Arc,
  thread::{park, Builder, JoinHandle, Thread},
};

use crate::{
  utils::{ToArc, UnsafeBorrow, UnsafeBorrowMut},
  Result,
};

use super::{BackgroundThread, Context, OneshotFulfill, SingleFn};
use crossbeam::{queue::SegQueue, utils::Backoff};

/**
 * The callback is called once per batch and the result is cloned to each
 * waiter — hence R: Clone.
 */
fn make_flush<'a, T, R>(
  mut when_buffered: SingleFn<'a, Vec<T>, R>,
) -> impl FnMut(&mut Vec<(T, Option<OneshotFulfill<Result<R>>>)>) -> bool + 'a
where
  T: Send + UnwindSafe + 'static,
  R: Send + Clone + 'static,
{
  move |buffered| {
    if buffered.is_empty() {
      return false;
    }

    let (values, waiting): (Vec<_>, Vec<_>) = buffered.drain(..).unzip();
    let result = when_buffered.call(values).map(Ok).unwrap_or_else(Err);
    waiting
      .into_iter()
      .flatten()
      .for_each(|done| done.fulfill(result.clone()));
    true
  }
}

/**
 * A background thread that batches incoming work items and flushes them
 * together via a single callback call.
 *
 * While a flush is in progress, new items accumulate in the queue.
 * After each flush, the thread immediately drains the queue for the next
 * batch — achieving continuous pipelining without idle gaps.
 *
 * The burst loop uses backoff to spin briefly before parking, allowing
 * items that arrive just after a flush to be included in the next batch
 * rather than triggering a separate wakeup.
 */
pub struct EagerBufferingThread<T, R> {
  queue: Arc<SegQueue<Context<T, R>>>,
  waker: Thread,
  /**
   * UnsafeCell allows taking the JoinHandle in close(&self).
   * Safe because close() is called at most once during shutdown.
   */
  handle: UnsafeCell<Option<JoinHandle<()>>>,
}
impl<T, R> EagerBufferingThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + Clone + 'static,
{
  pub fn new<S: ToString>(
    name: S,
    size: usize,
    count: usize,
    when_buffered: SingleFn<'static, Vec<T>, R>,
  ) -> Self {
    let queue = SegQueue::new().to_arc();
    let queue_c = Arc::clone(&queue);

    let handle = Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn(move || {
        let backoff = Backoff::new();
        let mut buffered = Vec::with_capacity(count);
        let mut flush = make_flush(when_buffered);

        loop {
          'burst: while !backoff.is_completed() {
            'inner: while buffered.len() < count {
              match queue_c.pop() {
                Some(Context::Work(v, done)) => buffered.push((v, Some(done))),
                Some(Context::Dispatch(v)) => buffered.push((v, None)),
                None => break 'inner,
                Some(Context::Term) => {
                  flush(&mut buffered);
                  return;
                }
              }
            }

            if flush(&mut buffered) {
              backoff.reset();
              continue 'burst;
            };
            backoff.snooze();
          }

          park();
          backoff.reset();
        }
      })
      .unwrap();
    let waker = handle.thread().clone();

    Self {
      queue,
      waker,
      handle: UnsafeCell::new(Some(handle)),
    }
  }
}
impl<T, R> UnwindSafe for EagerBufferingThread<T, R> {}
impl<T, R> RefUnwindSafe for EagerBufferingThread<T, R> {}
unsafe impl<T, R> Send for EagerBufferingThread<T, R> {}
unsafe impl<T, R> Sync for EagerBufferingThread<T, R> {}

impl<T, R> BackgroundThread<T, R> for EagerBufferingThread<T, R> {
  fn register(&self, ctx: Context<T, R>) -> bool {
    if self.handle.get().borrow_unsafe().is_none() {
      return false;
    }
    self.queue.push(ctx);
    self.waker.unpark();
    true
  }

  fn close(&self) {
    if let Some(th) = self.handle.get().borrow_mut_unsafe().take() {
      self.queue.push(Context::Term);
      self.waker.unpark();
      let _ = th.join();
    }
  }
}

#[cfg(test)]
#[path = "tests/eager.rs"]
mod tests;
