use std::{
  sync::Arc,
  thread::{park, Builder, Thread},
};

use crossbeam::{queue::SegQueue, utils::Backoff};

use crate::background::OneshotFulfill;

use super::{
  oneshot, Close, IdleQueue, Oneshot, SharedFn, ThreadId, ThreadSlot, UnwindSpawner,
};

enum Context<T, R> {
  Execute(T, OneshotFulfill<R>),
  #[allow(unused)]
  Dispatch(T),
  Term,
}

struct Inner<T, R> {
  queue: SegQueue<Context<T, R>>,
  idle: IdleQueue,
}
impl<T, R> Inner<T, R> {
  fn new(count: usize) -> Self {
    Self {
      queue: SegQueue::new(),
      idle: IdleQueue::new(count),
    }
  }
  fn try_park(&self, id: ThreadId) {
    if self.idle.try_park(id) {
      park();
    }
  }
  fn try_enqueue_idle(&self, id: ThreadId) {
    self.idle.try_enqueue(id);
  }
  fn wake_one(&self) -> Option<ThreadId> {
    self.idle.wake_one()
  }
}
pub struct SharedWorkThread<T, R> {
  inner: Arc<Inner<T, R>>,
  wakers: Box<[Thread]>,
  slots: Box<[ThreadSlot]>,
}
impl<T, R> SharedWorkThread<T, R> {
  const fn worker_loop(
    inner: Arc<Inner<T, R>>,
    thread_id: ThreadId,
    handler: SharedFn<'static, T, R>,
  ) -> impl FnOnce() {
    move || {
      let backoff = Backoff::new();
      loop {
        while !backoff.is_completed() {
          let Some(ctx) = inner.queue.pop() else {
            backoff.snooze();
            continue;
          };
          match ctx {
            Context::Execute(v, done) => done.fulfill(handler.call(v)),
            Context::Dispatch(v) => {
              let _ = handler.call(v);
            }
            Context::Term => return,
          }
          backoff.reset();
        }

        backoff.reset();
        inner.try_enqueue_idle(thread_id);
        let Some(ctx) = inner.queue.pop() else {
          inner.try_park(thread_id);
          continue;
        };
        match ctx {
          Context::Execute(v, done) => done.fulfill(handler.call(v)),
          Context::Dispatch(v) => {
            let _ = handler.call(v);
          }
          Context::Term => return,
        }
      }
    }
  }
  pub fn new(
    name: impl ToString,
    size: usize,
    count: usize,
    handler: SharedFn<'static, T, R>,
  ) -> Self
  where
    T: Send + 'static,
    R: Send + 'static,
  {
    let inner = Arc::new(Inner::new(count));
    let mut slots = Vec::with_capacity(count);
    let mut wakers = Vec::with_capacity(count);
    for i in 0..count {
      let handle = Builder::new()
        .name(name.to_string())
        .stack_size(size)
        .spawn_unwind(Self::worker_loop(inner.clone(), i, handler.clone()));
      wakers.push(handle.thread().clone());
      slots.push(ThreadSlot::new(handle));
    }

    Self {
      inner,
      slots: slots.into_boxed_slice(),
      wakers: wakers.into_boxed_slice(),
    }
  }

  pub fn execute(&self, value: T) -> Oneshot<R> {
    let (o, f) = oneshot();
    self.register(Context::Execute(value, f));
    o
  }
  #[allow(unused)]
  pub fn dispatch(&self, value: T) {
    self.register(Context::Dispatch(value));
  }
  fn register(&self, ctx: Context<T, R>) {
    self.inner.queue.push(ctx);
    if let Some(id) = self.inner.wake_one() {
      self.wakers[id].unpark();
    }
  }
}
impl<T: Send + 'static, R: Send + 'static> Close for SharedWorkThread<T, R> {
  fn close(&self) {
    let threads = self
      .slots
      .iter()
      .filter_map(|slot| slot.close())
      .collect::<Vec<_>>();
    for _ in 0..threads.len() {
      self.inner.queue.push(Context::Term);
    }
    for handle in threads {
      handle.thread().unpark();
      handle.join().unwrap();
    }
  }
}
