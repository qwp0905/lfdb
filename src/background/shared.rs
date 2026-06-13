use std::{
  mem::take,
  panic::{RefUnwindSafe, UnwindSafe},
  sync::{Arc, Mutex},
  thread::{park, Builder, JoinHandle, Thread},
};

use crossbeam::{
  atomic::AtomicCell,
  deque::{Injector, Stealer, Worker},
  queue::SegQueue,
  utils::Backoff,
};

use crate::{
  error,
  utils::{SBox, ShortenedMutex},
};

use super::{BackgroundThread, Context, SharedFn};

fn pop_or_steal<'a, A: 'a>(
  local: &Worker<A>,
  global: &Injector<A>,
  stealers: impl Iterator<Item = &'a Stealer<A>>,
) -> Option<A> {
  if let Some(task) = local.pop() {
    return Some(task);
  }
  if let Some(task) = global.steal_batch_and_pop(local).success() {
    return Some(task);
  }

  for stealer in stealers {
    if let Some(task) = stealer.steal().success() {
      return Some(task);
    }
  }
  None
}

fn drain_task<A>(global: &Injector<A>, local: &Worker<A>) {
  while let Some(ctx) = local.pop() {
    global.push(ctx);
  }
}

fn handle_task<T, R>(
  ctx: Context<T, R>,
  work: &SharedFn<'static, T, R>,
  name: &String,
) -> bool
where
  T: Send + UnwindSafe,
  R: Send,
{
  match ctx {
    Context::Work(v, done) => done.fulfill(work.call(v)),
    Context::Dispatch(v) => {
      if let Err(err) = work.call(v) {
        error!("error occurs in thread {}: {}", name, err);
      }
    }
    Context::Term => return false,
  }
  true
}

const fn worker_loop<T, R>(
  local: Worker<Context<T, R>>,
  global: Arc<Injector<Context<T, R>>>,
  stealers: Arc<Vec<Stealer<Context<T, R>>>>,
  idle: Arc<SegQueue<Idle>>,
  work: SharedFn<'static, T, R>,
  name: String,
  id: usize,
) -> impl FnOnce()
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  move || {
    let state = SBox::new(AtomicCell::new(State::Unqueued));

    let backoff = Backoff::new();
    let mut cycle = stealers.iter().cycle();
    let size = stealers.len();

    loop {
      while !backoff.is_completed() {
        let Some(ctx) = pop_or_steal(&local, &global, (&mut cycle).take(size)) else {
          backoff.snooze();
          continue;
        };

        if !handle_task(ctx, &work, &name) {
          return drain_task(&global, &local);
        }
        backoff.reset();
      }

      backoff.reset();
      if state
        .compare_exchange(State::Unqueued, State::Queued)
        .is_ok()
      {
        // there are no state in idle queue.
        idle.push(Idle::new(state.clone(), id));
      }

      let Some(ctx) = pop_or_steal(&local, &global, (&mut cycle).take(size)) else {
        if state.compare_exchange(State::Queued, State::Parked).is_ok() {
          park();
        }
        // if producer changed state, then never park.
        continue;
      };

      // enqueued but tasks are left. state will be changed by producer.
      if !handle_task(ctx, &work, &name) {
        return drain_task(&global, &local);
      }
    }
  }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
  Unqueued, // out of idle queue.
  Queued,   // in idle queue, but still working.
  Parked,   // in idle queue and parked.
}
struct Idle {
  state: SBox<AtomicCell<State>>,
  id: usize,
}
impl Idle {
  const fn new(state: SBox<AtomicCell<State>>, id: usize) -> Self {
    Self { state, id }
  }
}

/**
 * Multiple worker threads sharing a single channel for task distribution.
 * Suitable for tasks that require burst throughput but have long idle periods.
 */
pub struct SharedWorkThread<T, R = ()> {
  global: Arc<Injector<Context<T, R>>>,
  idle: Arc<SegQueue<Idle>>,
  wakers: Vec<Thread>,
  threads: Mutex<Vec<JoinHandle<()>>>,
  name: String,
  work: SharedFn<'static, T, R>,
}
impl<T, R> SharedWorkThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn new<S: ToString>(
    name: S,
    size: usize,
    count: usize,
    work: SharedFn<'static, T, R>,
  ) -> Self {
    let idle = Arc::new(SegQueue::new());
    let (stealers, workers): (Vec<_>, Vec<_>) = (0..count)
      .map(|_| Worker::<Context<T, R>>::new_fifo())
      .map(|w| (w.stealer(), w))
      .unzip();

    let global = Arc::new(Injector::new());
    let stealers = Arc::new(stealers);
    let mut threads = Vec::with_capacity(count);
    let mut wakers = Vec::with_capacity(count);
    let name = name.to_string();
    for (id, local) in workers.into_iter().enumerate() {
      let thread = Builder::new()
        .name(name.clone())
        .stack_size(size)
        .spawn(worker_loop(
          local,
          Arc::clone(&global),
          Arc::clone(&stealers),
          Arc::clone(&idle),
          work.clone(),
          name.clone(),
          id,
        ))
        .unwrap();

      wakers.push(thread.thread().clone());
      threads.push(thread);
    }

    Self {
      global,
      idle,
      wakers,
      threads: Mutex::new(threads),
      name,
      work,
    }
  }
}

unsafe impl<T, R> Send for SharedWorkThread<T, R> {}
unsafe impl<T, R> Sync for SharedWorkThread<T, R> {}
impl<T, R> RefUnwindSafe for SharedWorkThread<T, R> {}
impl<T, R> UnwindSafe for SharedWorkThread<T, R> {}

impl<T, R> BackgroundThread<T, R> for SharedWorkThread<T, R>
where
  T: Send + UnwindSafe,
  R: Send,
{
  fn register(&self, ctx: Context<T, R>) -> bool {
    self.global.push(ctx);

    let Some(idle) = self.idle.pop() else {
      return true;
    };

    if let State::Parked = idle.state.swap(State::Unqueued) {
      self.wakers[idle.id].unpark();
    }
    // if does not matches parked, worker thread are already working.
    true
  }

  fn close(&self) {
    let threads = take(&mut *self.threads.l());
    if threads.is_empty() {
      return;
    }

    for _ in 0..threads.len() {
      self.global.push(Context::Term);
    }
    for waker in &self.wakers {
      waker.unpark();
    }
    for th in threads {
      let _ = th.join();
    }

    while let Some(ctx) = self.global.steal().success() {
      handle_task(ctx, &self.work, &self.name);
    }
  }
}

#[cfg(test)]
#[path = "tests/shared.rs"]
mod tests;
