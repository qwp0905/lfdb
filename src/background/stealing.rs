use std::thread::{park, Builder, Thread};

use crossbeam::{
  atomic::AtomicCell,
  deque::{Injector, Stealer, Worker},
  queue::SegQueue,
  utils::Backoff,
};

use crate::{background::Oneshot, utils::SBox};

use super::{
  oneshot, Close, Dispatch, ExecutableContext, Execute, OneshotFulfill, SharedFn,
  ThreadSlot, TryWaitError, UnwindSpawner, WaitDisconnectedError,
};

/*
 * A worker that receives `Term` stops processing work. Any tasks already pulled
 * into its local queue are returned to the global injector so another worker,
 * or the close-time cleanup path, can handle them.
 */
fn handle_task<T, R>(
  ctx: ExecutableContext<T, R>,
  work: &SharedFn<'static, T, R>,
) -> bool {
  match ctx {
    ExecutableContext::Work(v, done) => done.fulfill(work.call(v)),
    ExecutableContext::Dispatch(v) => {
      let _ = work.call(v);
    }
    ExecutableContext::Term => return false,
  }
  true
}

const fn worker_loop<T, R>(
  local: Worker<ExecutableContext<T, R>>,
  core: SBox<Core<ExecutableContext<T, R>>>,
  work: SharedFn<'static, T, R>,
  id: usize,
) -> impl FnOnce()
where
  T: Send + 'static,
  R: Send + 'static,
{
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

        if !handle_task(ctx, &work) {
          return core.drain_task(&local);
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
      if !handle_task(ctx, &work) {
        return core.drain_task(&local);
      }
    }
  }
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
struct Core<A> {
  global: Injector<A>,
  stealers: Vec<Stealer<A>>,
  idle: SegQueue<usize>,
  states: Vec<AtomicCell<State>>,
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
        stealers,
        idle: SegQueue::new(),
        states,
      },
      workers,
    )
  }
  fn wake_one(&self) -> Option<usize> {
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

  fn create_cycle(&self, id: usize) -> impl Iterator<Item = &'_ Stealer<A>> {
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

  fn try_park(&self, id: usize) {
    // if producer changed state, then never park.
    if self.states[id]
      .compare_exchange(State::Queued, State::Parked)
      .is_ok()
    {
      park();
    }
  }
  fn try_enqueue(&self, id: usize) {
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

  fn register(&self, value: A) -> Option<usize> {
    self.push_global(value);
    self.wake_one()
  }
  fn push_global(&self, value: A) {
    self.global.push(value);
  }
  fn pop_global(&self) -> Option<A> {
    self.global.steal().success()
  }

  fn steal_one(&self) -> Option<A> {
    self.stealers.iter().find_map(|s| s.steal().success())
  }
}

/**
 * Parallel background executor used by the engine.
 *
 * This is the engine's primary runtime for parallel background work. It uses a
 * work-stealing layout: producers push tasks into a global injector, workers
 * keep local queues, and idle workers can steal from each other when the global
 * queue is empty.
 *
 * When no work is available, workers register themselves in the idle queue and
 * park. Producers wake parked workers on demand, so the executor can handle
 * bursts of parallel work without keeping idle threads busy.
 */
pub struct StealingWorkThread<T, R = ()> {
  core: SBox<Core<ExecutableContext<T, R>>>,
  wakers: SBox<[Thread]>,
  threads: Vec<ThreadSlot>,
  work: SharedFn<'static, T, R>,
}
impl<T, R> StealingWorkThread<T, R> {
  pub fn new<S: ToString>(
    name: S,
    size: usize,
    count: usize,
    work: SharedFn<'static, T, R>,
  ) -> Self
  where
    T: Send + 'static,
    R: Send + 'static,
  {
    let (core, workers) = Core::new(count);
    let core = SBox::new(core);
    let mut threads = Vec::with_capacity(count);
    let mut wakers = Vec::with_capacity(count);
    let name = name.to_string();
    for (id, local) in workers.into_iter().enumerate() {
      let thread = Builder::new()
        .name(name.clone())
        .stack_size(size)
        .spawn_unwind(worker_loop(local, core.clone(), work.clone(), id));

      wakers.push(thread.thread().clone());
      threads.push(ThreadSlot::new(thread));
    }

    Self {
      core,
      wakers: SBox::from_boxed_slice(wakers.into_boxed_slice()),
      threads,
      work,
    }
  }

  fn register(&self, ctx: ExecutableContext<T, R>) {
    if let Some(id) = self.core.register(ctx) {
      self.wakers[id].unpark();
    }
  }

  fn coworker(&self) -> Coworker<T, R> {
    Coworker {
      core: self.core.clone(),
      work: self.work.clone(),
      wakers: self.wakers.clone(),
    }
  }

  pub fn create_pending<O>(&self) -> (PendingCoop<T, R, O>, OneshotFulfill<O>) {
    let (o, f) = oneshot();
    (PendingCoop::new(o, self.coworker()), f)
  }

  pub fn stream(&self) -> ForkStream<T, R> {
    ForkStream::new(self.coworker())
  }
}
impl<T: Send, R: Send> Close for StealingWorkThread<T, R> {
  /*
   * Closing a shared executor cannot use the same simple boundary as a single
   * worker. Tasks may already be in the global injector or in a worker's local
   * queue when `Term` is observed, and the exact ordering between task submission
   * and termination is distributed across workers.
   *
   * The executor therefore sends one `Term` per worker and, after all workers
   * have joined, drains the global queue on the closing thread. This preserves
   * the important guarantee: work submitted before `close` begins is completed
   * even if some workers encounter `Term` before processing all local work.
   */
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
      self.core.push_global(ExecutableContext::Term);
    }
    for th in threads {
      th.thread().unpark();
      th.join().unwrap();
    }

    while let Some(ctx) = self.core.pop_global() {
      handle_task(ctx, &self.work);
    }
  }
}
impl<T: Send, R: Send> Dispatch<T> for StealingWorkThread<T, R> {
  fn dispatch(&self, value: T) {
    self.register(ExecutableContext::Dispatch(value));
  }
}
impl<T: Send, R: Send> Execute<T, R> for StealingWorkThread<T, R> {
  fn execute(&self, value: T) -> super::Oneshot<R> {
    let (o, f) = oneshot();
    self.register(ExecutableContext::Work(value, f));
    o
  }
}
impl<T: Send, R: Send> StealingWorkThread<T, R> {
  pub fn cooperate(&self, value: T) -> PendingCoop<T, R> {
    PendingCoop::new(Execute::execute(self, value), self.coworker())
  }

  pub fn fork<I: ExactSizeIterator<Item = T>>(&self, values: I) -> ForkJoin<T, R> {
    let len = values.len();
    let mut receivers = Vec::with_capacity(len);
    for value in values {
      let (o, f) = oneshot();
      self.core.push_global(ExecutableContext::Work(value, f));
      receivers.push(o);
    }

    for id in (0..self.threads.len().min(len)).map_while(|_| self.core.wake_one()) {
      self.wakers[id].unpark();
    }

    ForkJoin::new(receivers, self.coworker())
  }
}

pub struct ForkStream<T, R>(ForkJoin<T, R>);
impl<T, R> ForkStream<T, R> {
  const fn new(coworker: Coworker<T, R>) -> Self {
    Self(ForkJoin::new(Vec::new(), coworker))
  }
  pub fn push(&mut self, value: T) {
    let (o, f) = oneshot();
    self.0.coworker.push(ExecutableContext::Work(value, f));
    self.0.receivers.push(o);
  }

  pub fn join(self) -> impl ExactSizeIterator<Item = R>
  where
    T: 'static,
    R: 'static,
  {
    self.0.join()
  }
}

pub struct PendingCoop<T, R, O = R> {
  recv: Oneshot<O>,
  coworker: Coworker<T, R>,
}
impl<T, R, O> PendingCoop<T, R, O> {
  const fn new(recv: Oneshot<O>, coworker: Coworker<T, R>) -> Self {
    Self { recv, coworker }
  }

  pub fn wait(self) -> std::result::Result<O, WaitDisconnectedError> {
    wait_with(&self.coworker, self.recv)
  }
}

fn wait_with<T, R, O>(
  coworker: &Coworker<T, R>,
  mut recv: Oneshot<O>,
) -> std::result::Result<O, WaitDisconnectedError> {
  loop {
    match recv.try_wait() {
      Ok(v) => return Ok(v),
      Err(TryWaitError::Disconnected) => return Err(WaitDisconnectedError),
      Err(TryWaitError::Empty(r)) => recv = r,
    }
    if !coworker.run() {
      return recv.wait_slow();
    }
  }
}

pub struct ForkJoin<T, R> {
  receivers: Vec<Oneshot<R>>,
  coworker: Coworker<T, R>,
}
impl<T, R> ForkJoin<T, R> {
  const fn new(receivers: Vec<Oneshot<R>>, coworker: Coworker<T, R>) -> Self {
    Self {
      receivers,
      coworker,
    }
  }

  pub fn join(self) -> impl ExactSizeIterator<Item = R>
  where
    T: 'static,
    R: 'static,
  {
    self
      .receivers
      .into_iter()
      .map(move |recv| wait_with(&self.coworker, recv).unwrap())
  }
}

struct Coworker<T, R> {
  core: SBox<Core<ExecutableContext<T, R>>>,
  work: SharedFn<'static, T, R>,
  wakers: SBox<[Thread]>,
}
impl<T, R> Coworker<T, R> {
  fn push(&self, ctx: ExecutableContext<T, R>) {
    if let Some(id) = self.core.register(ctx) {
      self.wakers[id].unpark();
    }
  }

  fn run(&self) -> bool {
    let Some(ctx) = self.core.pop_global().or_else(|| self.core.steal_one()) else {
      return false;
    };

    if handle_task(ctx, &self.work) {
      return true;
    }

    self.core.push_global(ExecutableContext::Term);
    for thread in self.wakers.iter() {
      thread.unpark();
    }
    false
  }
}

#[cfg(test)]
#[path = "tests/stealing.rs"]
mod tests;
