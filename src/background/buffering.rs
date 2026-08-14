use std::{
  sync::Arc,
  thread::{park, Builder, Thread},
};

use super::{
  oneshot, Close, Dispatch, ExecutableContext, Execute, OneshotFulfill, SingleFn,
  ThreadSlot, UnwindSpawner,
};

use crossbeam::{queue::SegQueue, utils::Backoff};

type Buffered<T, R> = Vec<(T, Option<OneshotFulfill<R>>)>;

/**
 * Flush buffered items by calling the batch handler once.
 *
 * The handler receives all buffered values and returns one result for the
 * entire batch. That result is cloned to every waiter that submitted a `Work`
 * item in the batch; dispatched items have no waiter.
 */
const fn make_flush<'a, T, R>(
  mut when_buffered: SingleFn<'a, Vec<T>, R>,
) -> impl FnMut(&mut Buffered<T, R>) -> bool + 'a
where
  T: Send + 'a,
  R: Send + Clone + 'a,
{
  move |buffered| {
    if buffered.is_empty() {
      return false;
    }

    let (values, waiting): (Vec<_>, Vec<_>) = buffered.drain(..).unzip();
    let result = when_buffered.call(values);
    waiting
      .into_iter()
      .flatten()
      .for_each(|done| done.fulfill(result.clone()));
    true
  }
}

const fn worker_loop<T, R>(
  queue: Arc<SegQueue<ExecutableContext<T, R>>>,
  count: usize,
  when_buffered: SingleFn<'static, Vec<T>, R>,
) -> impl FnOnce()
where
  T: Send,
  R: Send + Clone,
{
  move || {
    let backoff = Backoff::new();
    let mut buffered = Vec::with_capacity(count);
    let mut flush = make_flush(when_buffered);

    loop {
      while !backoff.is_completed() {
        for ctx in (0..count).map_while(|_| queue.pop()) {
          match ctx {
            ExecutableContext::Work(v, done) => buffered.push((v, Some(done))),
            ExecutableContext::Dispatch(v) => buffered.push((v, None)),
            ExecutableContext::Term => {
              flush(&mut buffered);
              return;
            }
          }
        }

        if flush(&mut buffered) {
          backoff.reset();
          continue;
        };
        backoff.snooze();
      }

      park();
      backoff.reset();
    }
  }
}

/**
 * Single-worker runtime that processes queued work in buffered batches.
 *
 * The worker drains up to `count` queued items, calls the handler once with the
 * collected `Vec<T>`, and completes all waiters with the returned result. While
 * one batch is being processed, producers can continue pushing new work into
 * the queue; after the flush, the worker immediately drains the next batch.
 */
pub struct BufferingThread<T, R> {
  queue: Arc<SegQueue<ExecutableContext<T, R>>>,
  waker: Thread,
  slot: ThreadSlot,
}
impl<T, R> BufferingThread<T, R> {
  pub fn new<S: ToString>(
    name: S,
    size: usize,
    count: usize,
    when_buffered: SingleFn<'static, Vec<T>, R>,
  ) -> Self
  where
    T: Send + 'static,
    R: Send + Clone + 'static,
  {
    let queue = Arc::new(SegQueue::new());
    let handle = Builder::new()
      .name(name.to_string())
      .stack_size(size)
      .spawn_unwind(worker_loop(queue.clone(), count, when_buffered));
    let waker = handle.thread().clone();
    Self {
      queue,
      waker,
      slot: ThreadSlot::new(handle),
    }
  }

  fn register(&self, ctx: ExecutableContext<T, R>) {
    self.queue.push(ctx);
    self.waker.unpark();
  }
}
impl<T: Send, R: Send> Close for BufferingThread<T, R> {
  fn close(&self) {
    if let Some(th) = self.slot.close() {
      self.queue.push(ExecutableContext::Term);
      self.waker.unpark();
      th.join().unwrap();
    }
  }
}
impl<T: Send, R: Send> Dispatch<T> for BufferingThread<T, R> {
  fn dispatch(&self, value: T) {
    self.register(ExecutableContext::Dispatch(value))
  }
}
impl<T: Send, R: Send> Execute<T, R> for BufferingThread<T, R> {
  fn execute(&self, value: T) -> super::Oneshot<R> {
    let (o, f) = oneshot();
    self.register(ExecutableContext::Work(value, f));
    o
  }
}

#[cfg(test)]
#[path = "tests/buffering.rs"]
mod buffering;
