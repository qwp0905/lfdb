use std::{
  iter::repeat,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
  thread::{park, Builder, Thread},
};

use crossbeam::{
  atomic::AtomicCell,
  channel::{unbounded, Receiver, Sender, TryRecvError},
  deque::{Injector, Stealer, Worker},
  queue::SegQueue,
  utils::Backoff,
};

use crate::utils::SBox;

use super::{
  into_task, PendingTask, SharedFn, TaskRef, ThreadSlot, TryWaitError, UnwindSpawner,
};

/*
 * Standard work-stealing priority:
 * 1. run local work first,
 * 2. pull a batch from the global injector,
 * 3. steal from other workers as a fallback.
 */
fn pop_or_steal<'a, A: 'a>(
  local: &Worker<A>,
  global: &Injector<A>,
  mut stealers: impl Iterator<Item = &'a Stealer<A>>,
) -> Option<A> {
  if let Some(task) = local.pop() {
    return Some(task);
  }
  if let Some(task) = global.steal_batch_and_pop(local).success() {
    return Some(task);
  }
  stealers.find_map(|stealer| stealer.steal().success())
}

fn drain_task<A>(global: &Injector<A>, local: &Worker<A>) {
  while let Some(ctx) = local.pop() {
    global.push(ctx);
  }
}

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
struct Idle {
  state: SBox<AtomicCell<State>>,
  id: usize,
}
impl Idle {
  const fn new(state: SBox<AtomicCell<State>>, id: usize) -> Self {
    Self { state, id }
  }
}

const fn worker_loop(
  local: Worker<Context>,
  queue: SBox<Queue>,
  idle: SBox<SegQueue<Idle>>,
  id: usize,
) -> impl FnOnce() {
  move || {
    let state = SBox::new(AtomicCell::new(State::Unqueued));

    let backoff = Backoff::new();
    let mut cycle = (0..queue.stealers.len())
      .filter(|i| *i != id)
      .map(|i| &queue.stealers[i])
      .cycle();
    let size = queue.stealers.len() - 1;

    loop {
      while !backoff.is_completed() {
        let Some(ctx) = pop_or_steal(&local, &queue.global, (&mut cycle).take(size))
        else {
          backoff.snooze();
          continue;
        };

        match ctx {
          Context::Task(task_ref) => task_ref.run(),
          Context::Term => return drain_task(&queue.global, &local),
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

      let Some(ctx) = pop_or_steal(&local, &queue.global, (&mut cycle).take(size)) else {
        if state.compare_exchange(State::Queued, State::Parked).is_ok() {
          park();
        }
        // if producer changed state, then never park.
        continue;
      };

      // enqueued but tasks are left. state will be changed by producer.
      match ctx {
        Context::Task(task_ref) => task_ref.run(),
        Context::Term => return drain_task(&queue.global, &local),
      }
    }
  }
}

struct Queue {
  global: Injector<Context>,
  stealers: Vec<Stealer<Context>>,
}

const DEFAULT_STACK_SIZE: usize = 64 << 10;

pub struct ThreadPool {
  queue: SBox<Queue>,
  idle: SBox<SegQueue<Idle>>,
  wakers: SBox<Vec<Thread>>,
  threads: Vec<ThreadSlot>,
}
impl ThreadPool {
  pub fn new(count: usize) -> Self {
    let idle = SBox::new(SegQueue::new());
    let mut stealers = Vec::with_capacity(count);
    let mut workers = Vec::with_capacity(count);

    for _ in 0..count {
      let worker = Worker::<Context>::new_fifo();
      stealers.push(worker.stealer());
      workers.push(worker);
    }

    let queue = SBox::new(Queue {
      global: Injector::new(),
      stealers,
    });

    let mut threads = Vec::with_capacity(count);
    let mut wakers = Vec::with_capacity(count);
    for (id, local) in workers.into_iter().enumerate() {
      let thread = Builder::new()
        .name("thread spawner".to_string())
        .stack_size(DEFAULT_STACK_SIZE)
        .spawn_unwind(worker_loop(
          local,
          SBox::clone(&queue),
          SBox::clone(&idle),
          id,
        ));

      wakers.push(thread.thread().clone());
      threads.push(ThreadSlot::new(thread));
    }
    Self {
      queue,
      idle,
      wakers: SBox::new(wakers),
      threads,
    }
  }

  pub fn spawn<F, T>(&self, f: F) -> JoinHandle<T>
  where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
  {
    let (task, pending) = into_task(f);
    self.register_task(task);
    JoinHandle::new(pending, self.coworker())
  }

  fn coworker(&self) -> Coworker {
    Coworker::new(self.queue.clone(), self.wakers.clone())
  }

  fn register_task(&self, task: TaskRef) {
    self.queue.global.push(Context::Task(task));
    self.wake_one();
  }

  fn wake_one(&self) {
    let Some(idle) = self.idle.pop() else {
      return;
    };

    // if does not matches parked, worker thread are already working.
    if let State::Parked = idle.state.swap(State::Unqueued) {
      self.wakers[idle.id].unpark();
    }
  }

  fn fork_boxed<T, R>(
    self: &Arc<Self>,
    input: impl Iterator<Item = T>,
    concurrency: usize,
    handler: SharedFn<'static, T, R>,
  ) -> ForkJoin<R>
  where
    T: Send + 'static,
    R: Send + 'static,
  {
    ForkJoin::new(
      self.clone(),
      handler,
      input,
      concurrency.min(self.threads.len()),
    )
  }
  pub fn fork<T, R, F>(
    self: &Arc<Self>,
    input: impl Iterator<Item = T>,
    concurrency: usize,
    handler: F,
  ) -> ForkJoin<R>
  where
    T: Send + 'static,
    R: Send + 'static,
    F: Fn(T) -> R + Send + Sync + 'static,
  {
    self.fork_boxed(input, concurrency, SharedFn::new(Arc::new(handler)))
  }

  pub fn typed_executor<T, R, F>(
    self: &Arc<Self>,
    concurrency: usize,
    handler: F,
  ) -> TypedExecutor<T, R>
  where
    F: Fn(T) -> R + Send + Sync + 'static,
  {
    TypedExecutor::new(self.clone(), handler, concurrency.min(self.threads.len()))
  }

  pub fn close(&self) {
    let threads = self
      .threads
      .iter()
      .filter_map(|th| th.close())
      .collect::<Vec<_>>();
    if threads.is_empty() {
      return;
    }

    for _ in 0..threads.len() {
      self.queue.global.push(Context::Term);
    }
    for th in threads {
      th.thread().unpark();
      th.join().unwrap();
    }

    while let Some(ctx) = self.queue.global.steal().success() {
      match ctx {
        Context::Task(task_ref) => task_ref.run(),
        Context::Term => unreachable!(),
      }
    }
  }
}

struct Coworker {
  queue: SBox<Queue>,
  wakers: SBox<Vec<Thread>>,
}
impl Coworker {
  const fn new(queue: SBox<Queue>, wakers: SBox<Vec<Thread>>) -> Self {
    Self { queue, wakers }
  }

  fn cooperate(&self) -> bool {
    let Some(ctx) = self
      .queue
      .global
      .steal()
      .success()
      .or_else(|| self.queue.stealers.iter().find_map(|s| s.steal().success()))
    else {
      return false;
    };

    if let Context::Task(task_ref) = ctx {
      task_ref.run();
      return true;
    }

    self.queue.global.push(Context::Term);
    for thread in self.wakers.iter() {
      thread.unpark();
    }
    false
  }
}

pub struct JoinHandle<T> {
  task: PendingTask<T>,
  coworker: Coworker,
}
impl<T> JoinHandle<T> {
  const fn new(task: PendingTask<T>, coworker: Coworker) -> Self {
    Self { task, coworker }
  }

  pub fn wait(mut self) -> T {
    loop {
      match self.task.try_wait() {
        Ok(v) => return v,
        Err(TryWaitError::Disconnected) => unreachable!(),
        Err(TryWaitError::Empty(task)) => self.task = task,
      }

      if !self.coworker.cooperate() {
        return self.task.wait_slow().unwrap();
      }
    }
  }
}

pub struct ForkJoin<R> {
  stream: Receiver<R>,
  coworker: Coworker,
}
impl<R> ForkJoin<R> {
  fn new<T>(
    pool: Arc<ThreadPool>,
    handler: SharedFn<'static, T, R>,
    input: impl Iterator<Item = T>,
    concurrency: usize,
  ) -> Self
  where
    T: Send + 'static,
    R: Send + 'static,
  {
    let queue = SBox::new(SegQueue::new());
    let (result, stream) = unbounded();
    for task in input {
      queue.push(task);
    }
    for _ in 0..concurrency {
      Self::recursive(pool.clone(), queue.clone(), handler.clone(), result.clone());
    }
    Self {
      stream,
      coworker: pool.coworker(),
    }
  }

  fn recursive<T>(
    pool: Arc<ThreadPool>,
    queue: SBox<SegQueue<T>>,
    handler: SharedFn<'static, T, R>,
    result: Sender<R>,
  ) where
    T: Send + 'static,
    R: Send + 'static,
  {
    let cloned = Arc::clone(&pool);
    pool.spawn(move || {
      let Some(task) = queue.pop() else { return };
      let _ = result.send(handler.call(task));
      Self::recursive(cloned, queue, handler, result);
    });
  }

  pub fn join(mut self) -> impl Iterator<Item = R> {
    repeat(()).map_while(move |_| self.pop())
  }

  fn pop(&mut self) -> Option<R> {
    loop {
      match self.stream.try_recv() {
        Ok(v) => return Some(v),
        Err(TryRecvError::Disconnected) => return None,
        Err(TryRecvError::Empty) => {}
      }
      if !self.coworker.cooperate() {
        return self.stream.recv().ok();
      }
    }
  }
}

pub struct TypedExecutor<T, R> {
  pool: Arc<ThreadPool>,
  handler: SharedFn<'static, T, R>,
  concurrency: usize,
}
impl<T, R> TypedExecutor<T, R> {
  fn new<F>(pool: Arc<ThreadPool>, handler: F, concurrency: usize) -> Self
  where
    F: Fn(T) -> R + Send + Sync + 'static,
  {
    Self {
      pool,
      handler: SharedFn::new(Arc::new(handler)),
      concurrency,
    }
  }
  pub fn fork(&self, input: impl Iterator<Item = T>) -> ForkJoin<R>
  where
    T: Send + 'static,
    R: Send + 'static,
  {
    self
      .pool
      .fork_boxed(input, self.concurrency, self.handler.clone())
  }

  pub fn execute(&self, value: T) -> JoinHandle<R>
  where
    T: Send + 'static,
    R: Send + 'static,
  {
    let handler = self.handler.clone();
    self.pool.spawn(move || handler.call(value))
  }
  pub fn dispatch(&self, value: T)
  where
    T: Send + 'static,
    R: Send + 'static,
  {
    self.execute(value);
  }

  pub fn stream(&self) -> ForkStream<T, R> {
    ForkStream::new(self.pool.clone(), self.concurrency, self.handler.clone())
  }
}
impl<T, R> Clone for TypedExecutor<T, R> {
  fn clone(&self) -> Self {
    Self {
      pool: self.pool.clone(),
      handler: self.handler.clone(),
      concurrency: self.concurrency,
    }
  }
}

struct StreamState<T, R> {
  pool: Arc<ThreadPool>,
  handler: SharedFn<'static, T, R>,
  input: SegQueue<T>,
  output: Sender<R>,
  active: AtomicUsize,
}
impl<T, R> StreamState<T, R> {
  const fn new(
    pool: Arc<ThreadPool>,
    handler: SharedFn<'static, T, R>,
    output: Sender<R>,
    concurrency: usize,
  ) -> Self {
    Self {
      pool,
      handler,
      input: SegQueue::new(),
      output,
      active: AtomicUsize::new(concurrency),
    }
  }
  fn activate(self: &Arc<Self>)
  where
    T: Send + 'static,
    R: Send + 'static,
  {
    if self
      .active
      .fetch_update(Ordering::Release, Ordering::Acquire, |active| {
        active.checked_sub(1)
      })
      .is_err()
    {
      return;
    }

    let state = self.clone();
    self.pool.spawn(move || state.drain());
  }

  fn drain(self: &Arc<Self>)
  where
    T: Send + 'static,
    R: Send + 'static,
  {
    while let Some(input) = self.input.pop() {
      let _ = self.output.send(self.handler.call(input));
    }
    self.active.fetch_add(1, Ordering::AcqRel);
    if !self.input.is_empty() {
      self.activate();
    }
  }
}

pub struct ForkStream<T, R> {
  state: Arc<StreamState<T, R>>,
  output: Receiver<R>,
}
impl<T, R> ForkStream<T, R> {
  fn new(
    pool: Arc<ThreadPool>,
    concurrency: usize,
    handler: SharedFn<'static, T, R>,
  ) -> Self {
    let (output_sender, output) = unbounded();
    let state = Arc::new(StreamState::new(pool, handler, output_sender, concurrency));
    Self { state, output }
  }
  pub fn push(&self, value: T)
  where
    T: Send + 'static,
    R: Send + 'static,
  {
    self.state.input.push(value);
    self.state.activate();
  }
  pub fn join(self) -> impl Iterator<Item = R> {
    let coworker = self.state.pool.coworker();
    let stream = self.output;
    ForkJoin { stream, coworker }.join()
  }
}
