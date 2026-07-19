use std::{
  cell::Cell,
  io::{Error, IoSlice, Result},
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
};

use crossbeam::queue::SegQueue;

use super::{max_iov, IOBackend};
use crate::{
  background::{oneshot, BackgroundThread, Oneshot, OneshotFulfill},
  measure,
  metrics::MetricsRegistry,
  utils::{ExclusivePin, ExclusiveToken, SBox, SharedToken},
};

pub type WriteTask = (u64, IoSlice<'static>);

pub struct AllocState(Cell<u64>);
impl AllocState {
  pub const fn new(allocated: u64) -> Self {
    Self(Cell::new(allocated))
  }
  pub const fn get(&self) -> u64 {
    self.0.get()
  }
  pub fn set(&self, allocated: u64) {
    self.0.set(allocated);
  }
}

/**
 * Tracks the file size already covered by preallocation.
 *
 * `AllocState` uses `Cell` because it is only accessed by the single worker that
 * owns the corresponding `AllocAndWrite` flush pass. The value is stored in an
 * `SBox` and can move across threads, but `TaskPublisher::occupied` serializes
 * execution so the state is logically single-threaded.
 */
unsafe impl Send for AllocState {}
unsafe impl Sync for AllocState {}

pub struct HandleState {
  /**
   * Pin to protect file I/O from truncate.
   */
  pin: ExclusivePin,
  /**
   * Flag to check for file existence a little faster.
   */
  closed: AtomicBool,
}
impl HandleState {
  pub const fn new() -> Self {
    Self {
      pin: ExclusivePin::new(),
      closed: AtomicBool::new(false),
    }
  }

  pub fn is_closed(&self) -> bool {
    self.closed.load(Ordering::Acquire)
  }

  pub fn try_shared(&self) -> Option<SharedToken<'_>> {
    self.pin.try_shared()
  }
  pub fn try_exclusive(&self) -> Option<ExclusiveToken<'_>> {
    self.pin.try_exclusive()
  }
}

/**
 * Per-handle task publisher and batching gate.
 *
 * Callers push requests into the queue concurrently. The `occupied` flag elects
 * exactly one caller as the winner that dispatches this publisher to the IO
 * worker pool. That worker owns the flush pass, drains the buffered requests,
 * and may keep ownership while more requests arrive.
 */
pub struct TaskPublisher<T> {
  queue: SegQueue<(T, OneshotFulfill<Result<()>>)>,
  occupied: AtomicBool,
}
impl<T> TaskPublisher<T> {
  pub const fn new() -> Self {
    Self {
      queue: SegQueue::new(),
      occupied: AtomicBool::new(false),
    }
  }
}
impl SBox<TaskPublisher<WriteTask>> {
  /**
   * For files whose usable space grows with writes, such as table segments.
   * The worker preallocates up to the highest required offset before writing.
   */
  pub fn publish_alloc_and_write(
    &self,
    state: &SBox<HandleState>,
    thread: &IOThread,
    backend: &Arc<dyn IOBackend>,
    alloc: &SBox<AllocState>,
    task: WriteTask,
  ) -> Oneshot<Result<()>> {
    if state.is_closed() {
      return Oneshot::fulfilled(Ok(()));
    }

    let (o, f) = oneshot();
    self.queue.push((task, f));
    if self.occupied.fetch_or(true, Ordering::Release) {
      return o;
    }

    thread.dispatch((
      backend.clone(),
      IOTask::Write(self.clone(), Some(alloc.clone())),
      state.clone(),
    ));
    o
  }
  /**
   * For fixed-size or externally preallocated files.
   * The worker only batches and writes; allocation is handled outside.
   */
  pub fn publish_write_only(
    &self,
    state: &SBox<HandleState>,
    thread: &IOThread,
    backend: &Arc<dyn IOBackend>,
    task: WriteTask,
  ) -> Oneshot<Result<()>> {
    if state.is_closed() {
      return Oneshot::fulfilled(Ok(()));
    }

    let (o, f) = oneshot();
    self.queue.push((task, f));
    if self.occupied.fetch_or(true, Ordering::Release) {
      return o;
    }

    thread.dispatch((
      backend.clone(),
      IOTask::Write(self.clone(), None),
      state.clone(),
    ));
    o
  }

  const MAX_FLUSH_COUNT: usize = max_iov();
  fn handle_write(
    &self,
    metrics: &MetricsRegistry,
    backend: &dyn IOBackend,
    state: &HandleState,
    alloc: Option<&AllocState>,
  ) {
    let mut buffered = Vec::with_capacity(Self::MAX_FLUSH_COUNT);

    loop {
      for task in (0..Self::MAX_FLUSH_COUNT).map_while(|_| self.queue.pop()) {
        buffered.push(task);
      }
      flush_write(metrics, backend, state, &mut buffered, alloc);

      // Drop ownership before checking the queue so a new publisher can schedule a
      // worker. If work is already visible after that, try to reacquire ownership and
      // continue draining it here. Losing the race means another worker was scheduled.
      self.occupied.fetch_and(false, Ordering::Release);
      if self.queue.is_empty() {
        break;
      }
      if self.occupied.fetch_or(true, Ordering::AcqRel) {
        break;
      }
    }
  }
}

impl SBox<TaskPublisher<()>> {
  /**
   * Batches multiple fdatasync waiters into one syscall. A sync batch has one
   * result, so every waiter receives the same completion result.
   */
  pub fn publish_sync(
    &self,
    state: &SBox<HandleState>,
    thread: &IOThread,
    backend: &Arc<dyn IOBackend>,
  ) -> Oneshot<Result<()>> {
    if state.is_closed() {
      return Oneshot::fulfilled(Ok(()));
    }

    let (o, f) = oneshot();
    self.queue.push(((), f));
    if self.occupied.fetch_or(true, Ordering::Release) {
      return o;
    }

    thread.dispatch((backend.clone(), IOTask::Sync(self.clone()), state.clone()));
    o
  }

  const MAX_FLUSH_COUNT: usize = 512;
  fn handle_sync(
    &self,
    metrics: &MetricsRegistry,
    backend: &dyn IOBackend,
    state: &HandleState,
  ) {
    let mut buffered = Vec::with_capacity(Self::MAX_FLUSH_COUNT);

    loop {
      for (_, fulfill) in (0..Self::MAX_FLUSH_COUNT).map_while(|_| self.queue.pop()) {
        buffered.push(fulfill);
      }

      flush_fdatasync(metrics, backend, state, &mut buffered);
      self.occupied.fetch_and(false, Ordering::Release);
      if self.queue.is_empty() {
        break;
      }
      if self.occupied.fetch_or(true, Ordering::AcqRel) {
        break;
      }
    }
  }
}
pub enum IOTask {
  Write(SBox<TaskPublisher<WriteTask>>, Option<SBox<AllocState>>),
  Sync(SBox<TaskPublisher<()>>),
}
type ThreadArg = (Arc<dyn IOBackend>, IOTask, SBox<HandleState>);
pub type IOThread = BackgroundThread<ThreadArg, ()>;

pub fn create_io_thread(metrics: Arc<MetricsRegistry>) -> impl Fn(ThreadArg) {
  move |(backend, task, state)| {
    metrics.active_io_threads.inc();
    match task {
      IOTask::Write(handle, alloc) => {
        handle.handle_write(&metrics, &*backend, &state, alloc.as_deref())
      }
      IOTask::Sync(handle) => handle.handle_sync(&metrics, &*backend, &state),
    }
    metrics.active_io_threads.dec();
  }
}
fn flush_write(
  metrics: &MetricsRegistry,
  backend: &dyn IOBackend,
  state: &HandleState,
  buffered: &mut Vec<(WriteTask, OneshotFulfill<Result<()>>)>,
  alloc: Option<&AllocState>,
) {
  if buffered.is_empty() {
    return;
  }

  let (values, waiting): (Vec<_>, Vec<_>) = buffered.drain(..).unzip();
  let Some(_token) = state.pin.try_shared() else {
    // If truncate/remove owns the handle exclusively, this request no longer has a
    // meaningful file to operate on. Mark the handle closed and complete queued
    // waiters as successful no-ops.
    state.closed.fetch_or(true, Ordering::Release);
    return waiting.into_iter().for_each(|done| done.fulfill(Ok(())));
  };

  let result = exec_write(metrics, backend, values, alloc).map_err(|err| err.kind());
  metrics.disk_write_batch.record(waiting.len() as f64);
  waiting
    .into_iter()
    .for_each(|done| done.fulfill(result.map_err(Error::from)));
}

// Preallocate in coarse chunks so the filesystem can keep nearby writes in a
// more local extent instead of allocating space block by block. 1 MiB is a
// simple default chunk size, not a carefully tuned boundary.
const EXTENT: u64 = 1 << 20;
fn alloc_if_needed(
  required: u64,
  alloc: &AllocState,
  backend: &dyn IOBackend,
) -> Result<()> {
  let mut allocated = alloc.get();
  if allocated >= required {
    return Ok(());
  }
  while required >= allocated {
    allocated += EXTENT;
  }
  backend.fallocate(alloc.get(), allocated - alloc.get())?;
  alloc.set(allocated);
  Ok(())
}
fn exec_write(
  metrics: &MetricsRegistry,
  backend: &dyn IOBackend,
  mut buffered: Vec<WriteTask>,
  alloc: Option<&AllocState>,
) -> std::io::Result<()> {
  // Treat the flush batch like a tiny write buffer. When multiple writes target
  // the same byte range, only the last published value needs to reach the file.
  buffered.sort_by_key(|(i, _)| *i);
  buffered.reverse();
  buffered.dedup_by_key(|(i, b)| (*i, b.len()));
  buffered.reverse();

  if let Some(alloc) = alloc {
    // Space allocation is owned by this batching layer. Since all writes for this
    // handle are flushed here, the worker can preallocate once up to the highest
    // required offset before issuing the actual writes.
    let required = buffered.last().map(|(o, b)| *o + b.len() as u64).unwrap();
    alloc_if_needed(required, alloc, backend)?;
  }

  for chunk in buffered.chunk_by(|(a_o, a_b), (b_o, _)| a_o + a_b.len() as u64 == *b_o) {
    let (offset, bufs): (Vec<_>, Vec<_>) = chunk.iter().map(|(o, b)| (*o, *b)).unzip();
    let offset = offset[0];
    if bufs.len() == 1 {
      measure!(metrics.disk_write, backend.pwrite_or_fail(&bufs[0], offset))?;
      continue;
    }

    measure!(metrics.disk_write, backend.pwritev_or_fail(&bufs, offset))?;
  }
  Ok(())
}

fn flush_fdatasync(
  metrics: &MetricsRegistry,
  backend: &dyn IOBackend,
  state: &HandleState,
  waiting: &mut Vec<OneshotFulfill<Result<()>>>,
) {
  if waiting.is_empty() {
    return;
  }

  let Some(_token) = state.pin.try_shared() else {
    state.closed.fetch_or(true, Ordering::Release);
    return waiting.drain(..).for_each(|done| done.fulfill(Ok(())));
  };

  let result = backend.fdatasync().map_err(|err| err.kind());
  metrics.disk_sync_batch.record(waiting.len() as f64);
  waiting
    .drain(..)
    .for_each(|done| done.fulfill(result.map_err(Error::from)));
}
