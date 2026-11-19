use std::{sync::Arc, thread::Builder};

use crossbeam::{queue::SegQueue, utils::Backoff};

use super::{SharedFn, ThreadParker, ThreadSlot, UnwindSpawner};

enum Context<T> {
  Dispatch(T),
  Term,
}

struct Inner<T> {
  queue: SegQueue<Context<T>>,
  parker: ThreadParker,
}
impl<T> Inner<T> {
  fn new() -> Self {
    Self {
      queue: SegQueue::new(),
      parker: ThreadParker::new(),
    }
  }
}
pub struct SharedWorkThread<T: Send + 'static> {
  inner: Arc<Inner<T>>,
  slots: Box<[ThreadSlot]>,
}
impl<T: Send + 'static> SharedWorkThread<T> {
  const fn worker_loop(
    inner: Arc<Inner<T>>,
    handler: SharedFn<'static, T, ()>,
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
            Context::Dispatch(v) => handler.call(v),
            Context::Term => return,
          }
          backoff.reset();
        }

        backoff.reset();
        let Some(ctx) = inner.parker.park_or_cancel(|| inner.queue.pop()) else {
          continue;
        };
        match ctx {
          Context::Dispatch(v) => handler.call(v),
          Context::Term => return,
        }
      }
    }
  }
  pub fn new(
    name: impl ToString,
    size: usize,
    count: usize,
    handler: SharedFn<'static, T, ()>,
  ) -> Self
  where
    T: Send + 'static,
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

  pub fn dispatch(&self, value: T) {
    self.register(Context::Dispatch(value));
  }
  fn register(&self, ctx: Context<T>) {
    self.inner.queue.push(ctx);
    self.inner.parker.wake_once();
  }
  pub fn close(&self) {
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
impl<T: Send + 'static> Drop for SharedWorkThread<T> {
  fn drop(&mut self) {
    self.close();
  }
}
