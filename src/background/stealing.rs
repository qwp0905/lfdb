use std::thread::{park, Builder, Thread};

use crossbeam::{
  atomic::AtomicCell,
  deque::{Injector, Stealer, Worker},
  queue::SegQueue,
  utils::Backoff,
};

use crate::utils::SBox;

use super::{Context, SharedFn, ThreadSlot, UnwindSpawner};

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

/*
 * A worker that receives `Term` stops processing work. Any tasks already pulled
 * into its local queue are returned to the global injector so another worker,
 * or the close-time cleanup path, can handle them.
 */
fn handle_task<T, R>(ctx: Context<T, R>, work: &SharedFn<'static, T, R>) -> bool {
  match ctx {
    Context::Work(v, done) => done.fulfill(work.call(v)),
    Context::Dispatch(v) => {
      let _ = work.call(v);
    }
    Context::Term => return false,
  }
  true
}

const fn worker_loop<T, R>(
  local: Worker<Context<T, R>>,
  global: SBox<Injector<Context<T, R>>>,
  stealers: SBox<Vec<Stealer<Context<T, R>>>>,
  idle: SBox<SegQueue<Idle>>,
  work: SharedFn<'static, T, R>,
  id: usize,
) -> impl FnOnce()
where
  T: Send + 'static,
  R: Send + 'static,
{
  move || {
    let state = SBox::new(AtomicCell::new(State::Unqueued));

    let backoff = Backoff::new();
    let mut cycle = (0..stealers.len())
      .filter(|i| *i != id)
      .map(|i| &stealers[i])
      .cycle();
    let size = stealers.len() - 1;

    loop {
      while !backoff.is_completed() {
        let Some(ctx) = pop_or_steal(&local, &global, (&mut cycle).take(size)) else {
          backoff.snooze();
          continue;
        };

        if !handle_task(ctx, &work) {
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
      if !handle_task(ctx, &work) {
        return drain_task(&global, &local);
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
  global: SBox<Injector<Context<T, R>>>,
  idle: SBox<SegQueue<Idle>>,
  wakers: SBox<Vec<Thread>>,
  threads: Vec<ThreadSlot>,
  stealers: SBox<Vec<Stealer<Context<T, R>>>>,
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
    let idle = SBox::new(SegQueue::new());
    let mut stealers = Vec::with_capacity(count);
    let mut workers = Vec::with_capacity(count);

    for _ in 0..count {
      let worker = Worker::<Context<T, R>>::new_fifo();
      stealers.push(worker.stealer());
      workers.push(worker);
    }

    let global = SBox::new(Injector::new());
    let stealers = SBox::new(stealers);
    let mut threads = Vec::with_capacity(count);
    let mut wakers = Vec::with_capacity(count);
    let name = name.to_string();
    for (id, local) in workers.into_iter().enumerate() {
      let thread = Builder::new()
        .name(name.clone())
        .stack_size(size)
        .spawn_unwind(worker_loop(
          local,
          SBox::clone(&global),
          SBox::clone(&stealers),
          SBox::clone(&idle),
          work.clone(),
          id,
        ));

      wakers.push(thread.thread().clone());
      threads.push(ThreadSlot::new(thread));
    }
    Self {
      global,
      idle,
      wakers: SBox::new(wakers),
      threads,
      stealers,
      work,
    }
  }

  pub fn register(&self, ctx: Context<T, R>) {
    self.global.push(ctx);

    let Some(idle) = self.idle.pop() else {
      return;
    };

    // if does not matches parked, worker thread are already working.
    if let State::Parked = idle.state.swap(State::Unqueued) {
      self.wakers[idle.id].unpark();
    }
  }

  pub fn coworker(&self) -> Coworker<T, R> {
    Coworker {
      global: SBox::clone(&self.global),
      stealers: SBox::clone(&self.stealers),
      work: self.work.clone(),
      wakers: SBox::clone(&self.wakers),
    }
  }

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
      self.global.push(Context::Term);
    }
    for th in threads {
      th.thread().unpark();
      th.join().unwrap();
    }

    while let Some(ctx) = self.global.steal().success() {
      handle_task(ctx, &self.work);
    }
  }
}

pub struct Coworker<T, R> {
  global: SBox<Injector<Context<T, R>>>,
  stealers: SBox<Vec<Stealer<Context<T, R>>>>,
  work: SharedFn<'static, T, R>,
  wakers: SBox<Vec<Thread>>,
}
impl<T, R> Coworker<T, R> {
  pub fn run(&self) -> bool {
    let Some(ctx) = self
      .global
      .steal()
      .success()
      .or_else(|| self.stealers.iter().find_map(|s| s.steal().success()))
    else {
      return false;
    };

    if handle_task(ctx, &self.work) {
      return true;
    }

    self.global.push(Context::Term);
    for thread in self.wakers.iter() {
      thread.unpark();
    }
    false
  }
}

#[cfg(test)]
#[path = "tests/stealing.rs"]
mod tests;
