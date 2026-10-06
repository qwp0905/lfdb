use std::{sync::Arc, thread::Builder};

use crossbeam::{queue::SegQueue, utils::Backoff};

use super::{
  create_parker, oneshot, Close, Oneshot, OneshotFulfill, SharedFn, ThreadParker,
  ThreadSlot, UnwindSpawner,
};

enum Context<T, R> {
  Execute(T, OneshotFulfill<R>),
  #[allow(unused)]
  Dispatch(T),
  Term,
}

struct Inner<T, R> {
  queue: SegQueue<Context<T, R>>,
  parker: ThreadParker,
}
impl<T, R> Inner<T, R> {
  fn new() -> Self {
    Self {
      queue: SegQueue::new(),
      parker: create_parker!(),
    }
  }
  // fn try_park(&self, id: ThreadId) {
  //   if self.parker.try_park(id) {
  //     park();
  //   }
  // }
  // fn try_enqueue_idle(&self, id: ThreadId) {
  //   self.parker.try_enqueue(id);
  // }
  // fn wake_one(&self) -> Option<ThreadId> {
  //   self.parker.wake_one()
  // }
}
pub struct SharedWorkThread<T, R> {
  inner: Arc<Inner<T, R>>,
  slots: Box<[ThreadSlot]>,
}
impl<T, R> SharedWorkThread<T, R> {
  const fn worker_loop(
    inner: Arc<Inner<T, R>>,
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
        let Some(ctx) = inner.parker.park_or_cancel(|| inner.queue.pop()) else {
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
    let inner = Arc::new(Inner::new());
    let mut slots = Vec::with_capacity(count);
    for _ in 0..count {
      let handle = Builder::new()
        .name(name.to_string())
        .stack_size(size)
        .spawn_unwind(Self::worker_loop(inner.clone(), handler.clone()));
      slots.push(ThreadSlot::new(handle));
    }

    Self {
      inner,
      slots: slots.into_boxed_slice(),
    }
  }

  pub fn execute(&self, value: T) -> Oneshot<R> {
    let (o, f) = oneshot();
    self.register(Context::Execute(value, f));
    o
  }
  #[cfg(not(target_os = "linux"))]
  pub fn dispatch(&self, value: T) {
    self.register(Context::Dispatch(value));
  }
  fn register(&self, ctx: Context<T, R>) {
    self.inner.queue.push(ctx);
    self.inner.parker.wake_once();
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
    self.inner.parker.wake_all();
    for handle in threads {
      handle.join().unwrap();
    }
  }
}
