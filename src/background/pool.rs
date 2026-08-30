use std::{
  sync::Arc,
  thread::{park, Builder, Thread},
};

use crossbeam::{
  atomic::AtomicCell,
  deque::{Injector, Stealer, Worker},
  queue::SegQueue,
  utils::Backoff,
};

use super::{
  into_task, Close, PendingTask, SharedFn, TaskRef, ThreadSlot, UnwindSpawner,
};
use crate::utils::SBox;

type ThreadId = usize;

enum Context {
  Task(TaskRef),
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

const fn worker_loop(
  local: Worker<Context>,
  core: SBox<Core<Context>>,
  id: ThreadId,
) -> impl FnOnce() {
  move || {
    let backoff = Backoff::new();
    let size = core.size() - 1;
    let mut cycle = core.create_cycle(id);

    loop {
      while !backoff.is_completed() {
        let Some(ctx) = core.pop_or_steal(&local, (&mut cycle).take(size)) else {
          backoff.snooze();
          continue;
        };

        match ctx {
          Context::Task(task_ref) => task_ref.run(),
          Context::Term => return core.drain_task(&local),
        }
        backoff.reset();
      }

      backoff.reset();
      core.try_enqueue(id);
      let Some(ctx) = core.pop_or_steal(&local, (&mut cycle).take(size)) else {
        core.try_park(id);
        continue;
      };

      // enqueued but tasks are left. state will be changed by producer.
      match ctx {
        Context::Task(task_ref) => task_ref.run(),
        Context::Term => return core.drain_task(&local),
      }
    }
  }
}

struct Core<A> {
  global: Injector<A>,
  stealers: Box<[Stealer<A>]>,
  idle: SegQueue<ThreadId>,
  states: Box<[AtomicCell<State>]>,
}
impl<A> Core<A> {
  fn new(count: usize) -> (Self, Vec<Worker<A>>) {
    let mut workers = Vec::with_capacity(count);
    let mut stealers = Vec::with_capacity(count);
    let mut states = Vec::with_capacity(count);

    for _ in 0..count {
      let worker = Worker::new_fifo();
      stealers.push(worker.stealer());
      workers.push(worker);
      states.push(AtomicCell::new(State::Unqueued));
    }

    (
      Self {
        global: Injector::new(),
        stealers: stealers.into_boxed_slice(),
        idle: SegQueue::new(),
        states: states.into_boxed_slice(),
      },
      workers,
    )
  }
  fn wake_one(&self) -> Option<ThreadId> {
    let id = self.idle.pop()?;
    // if does not matches parked, worker thread are already working.
    if let State::Parked = self.states[id].swap(State::Unqueued) {
      return Some(id);
    }

    None
  }
  const fn size(&self) -> usize {
    self.stealers.len()
  }

  fn create_cycle(&self, id: ThreadId) -> impl Iterator<Item = &'_ Stealer<A>> {
    (0..self.size())
      .filter(move |i| *i != id)
      .map(|i| &self.stealers[i])
      .cycle()
  }

  fn drain_task(&self, local: &Worker<A>) {
    while let Some(ctx) = local.pop() {
      self.global.push(ctx);
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
  fn try_enqueue(&self, id: ThreadId) {
    if self.states[id]
      .compare_exchange(State::Unqueued, State::Queued)
      .is_ok()
    {
      // there are no state in idle queue.
      self.idle.push(id);
    }
  }

  /*
   * Standard work-stealing priority:
   * 1. run local work first,
   * 2. pull a batch from the global injector,
   * 3. steal from other workers as a fallback.
   */
  fn pop_or_steal<'a>(
    &self,
    local: &Worker<A>,
    mut stealers: impl Iterator<Item = &'a Stealer<A>>,
  ) -> Option<A>
  where
    A: 'a,
  {
    if let Some(task) = local.pop() {
      return Some(task);
    }
    if let Some(task) = self.global.steal_batch_and_pop(local).success() {
      return Some(task);
    }

    stealers.find_map(|stealer| stealer.steal().success())
  }

  fn push_global(&self, value: A) {
    self.global.push(value);
  }
  fn pop_global(&self) -> Option<A> {
    self.global.steal().success()
  }
}

pub struct ThreadPool {
  core: SBox<Core<Context>>,
  wakers: SBox<[Thread]>,
  threads: Vec<ThreadSlot>,
}
impl ThreadPool {
  pub fn new<S: ToString>(name: S, size: usize, count: usize) -> Self {
    let (core, workers) = Core::new(count);
    let core = SBox::new(core);
    let mut threads = Vec::with_capacity(count);
    let mut wakers = Vec::with_capacity(count);
    let name = name.to_string();
    for (id, local) in workers.into_iter().enumerate() {
      let thread = Builder::new()
        .name(name.clone())
        .stack_size(size)
        .spawn_unwind(worker_loop(local, core.clone(), id));

      wakers.push(thread.thread().clone());
      threads.push(ThreadSlot::new(thread));
    }

    Self {
      core,
      wakers: SBox::from_boxed_slice(wakers.into_boxed_slice()),
      threads,
    }
  }

  pub fn spawn<F, T>(&self, f: F) -> PendingTask<T>
  where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
  {
    let (task, pending) = into_task(f);
    self.register_task(task);
    pending
  }

  pub fn fork<T, R, F, I>(&self, input: I, handler: F) -> ForkJoin<R>
  where
    I: ExactSizeIterator<Item = T>,
    T: Send + 'static,
    R: Send + 'static,
    F: Fn(T) -> R + Send + Sync + 'static,
  {
    let count = input.len();
    let mut pending = Vec::with_capacity(count);
    let handler = SharedFn::new(handler);
    for value in input {
      let handler = handler.clone();
      let (r, p) = into_task(move || handler.call(value));
      self.core.push_global(Context::Task(r));
      pending.push(p);
    }

    for _ in 0..self.threads.len().min(count) {
      self.wake_one();
    }

    ForkJoin::new(pending)
  }

  fn typed_executor<T, R, F>(self: &Arc<Self>, handler: F) -> TypedExecutor<T, R>
  where
    F: Fn(T) -> R + Send + Sync + 'static,
  {
    TypedExecutor::new(self.clone(), SharedFn::new(handler))
  }

  pub fn stream<T, R, F>(self: &Arc<Self>, handler: F) -> ForkStream<T, R>
  where
    F: Fn(T) -> R + Send + Sync + 'static,
  {
    ForkStream::new(self.typed_executor(handler))
  }

  fn register_task(&self, task: TaskRef) {
    self.core.push_global(Context::Task(task));
    self.wake_one();
  }

  fn wake_one(&self) {
    if let Some(id) = self.core.wake_one() {
      self.wakers[id].unpark();
    }
  }
}
impl Close for ThreadPool {
  fn close(&self) {
    let threads = self
      .threads
      .iter()
      .filter_map(|th| th.close())
      .collect::<Vec<_>>();
    if threads.is_empty() {
      return;
    }

    for _ in 0..threads.len() {
      self.core.push_global(Context::Term);
    }
    for th in threads {
      th.thread().unpark();
      th.join().unwrap();
    }

    while let Some(ctx) = self.core.pop_global() {
      if let Context::Task(task) = ctx {
        task.run();
      }
    }
  }
}

struct TypedExecutor<T, R> {
  pool: Arc<ThreadPool>,
  handler: SharedFn<'static, T, R>,
}
impl<T, R> TypedExecutor<T, R> {
  const fn new(pool: Arc<ThreadPool>, handler: SharedFn<'static, T, R>) -> Self {
    Self { pool, handler }
  }
  fn execute(&self, input: T) -> PendingTask<R>
  where
    T: Send + 'static,
    R: Send + 'static,
  {
    let handler = self.handler.clone();
    self.pool.spawn(move || handler.call(input))
  }
}

pub struct ForkStream<T, R> {
  executor: TypedExecutor<T, R>,
  inner: ForkJoin<R>,
}
impl<T, R> ForkStream<T, R> {
  const fn new(executor: TypedExecutor<T, R>) -> Self {
    Self {
      executor,
      inner: ForkJoin::new(Vec::new()),
    }
  }

  pub fn push(&mut self, input: T)
  where
    T: Send + 'static,
    R: Send + 'static,
  {
    let pending = self.executor.execute(input);
    self.inner.push(pending);
  }

  pub fn join(self) -> impl Iterator<Item = R> {
    self.inner.join()
  }
}

pub struct ForkJoin<T> {
  pending: Vec<PendingTask<T>>,
}
impl<T> ForkJoin<T> {
  const fn new(pending: Vec<PendingTask<T>>) -> Self {
    Self { pending }
  }

  fn push(&mut self, pending: PendingTask<T>) {
    self.pending.push(pending);
  }

  pub fn join(self) -> impl Iterator<Item = T> {
    self
      .pending
      .into_iter()
      .map(move |recv| recv.wait().unwrap())
  }
}
