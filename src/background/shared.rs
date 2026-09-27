use std::{
  sync::Arc,
  thread::{park, Builder, Thread},
};

use crossbeam::{atomic::AtomicCell, queue::SegQueue, utils::Backoff};

use crate::background::OneshotFulfill;

use super::{oneshot, Close, Oneshot, SharedFn, ThreadSlot, UnwindSpawner};

enum Context<T, R> {
  Execute(T, OneshotFulfill<R>),
  Dispatch(T),
  Term,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
  /*
   * The worker is not discoverable through the idle queue.
   *
   * Producers cannot wake this worker directly through `idle`; either it is
   * running, or it will re-register itself before sleeping.
   */
  Unqueued,
  /*
   * The worker has published itself to the idle queue and is preparing to park.
   *
   * It is still checking for work. If a producer observes this state and changes
   * it back to `Unqueued`, the worker will notice that signal and avoid parking.
   */
  Queued,
  /*
   * The worker found no work after publishing itself and has gone to sleep.
   *
   * A producer that takes this idle entry must unpark the corresponding thread.
   */
  Parked,
}

type ThreadId = usize;
struct Inner<T, R> {
  queue: SegQueue<Context<T, R>>,
  idle: SegQueue<ThreadId>,
  states: Box<[AtomicCell<State>]>,
}
impl<T, R> Inner<T, R> {
  fn new(count: usize) -> Self {
    let mut states = Vec::with_capacity(count);
    states.resize_with(count, || AtomicCell::new(State::Unqueued));
    Self {
      queue: SegQueue::new(),
      idle: SegQueue::new(),
      states: states.into_boxed_slice(),
    }
  }
  fn try_park(&self, id: ThreadId) {
    // if producer changed state, then never park.
    if self.states[id]
      .compare_exchange(State::Queued, State::Parked)
      .is_ok()
    {
      park();
    }
  }
  fn try_enqueue_idle(&self, id: ThreadId) {
    if self.states[id]
      .compare_exchange(State::Unqueued, State::Queued)
      .is_ok()
    {
      // there are no state in idle queue.
      self.idle.push(id);
    }
  }
  fn wake_one(&self) -> Option<ThreadId> {
    let id = self.idle.pop()?;
    // if does not matches parked, worker thread are already working.
    if let State::Parked = self.states[id].swap(State::Unqueued) {
      return Some(id);
    }
    None
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
