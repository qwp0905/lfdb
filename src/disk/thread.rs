use std::{
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
  pub fn publish_write(
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

    thread.dispatch((backend.clone(), IOTask::Write(self.clone()), state.clone()));
    o
  }

  const MAX_FLUSH_COUNT: usize = max_iov();
  fn handle_write(
    &self,
    metrics: &MetricsRegistry,
    backend: &dyn IOBackend,
    state: &HandleState,
  ) {
    let mut buffered = Vec::with_capacity(Self::MAX_FLUSH_COUNT);

    loop {
      for task in (0..Self::MAX_FLUSH_COUNT).map_while(|_| self.queue.pop()) {
        buffered.push(task);
      }

      flush_write(metrics, backend, state, &mut buffered);
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
  Write(SBox<TaskPublisher<WriteTask>>),
  Sync(SBox<TaskPublisher<()>>),
}
type ThreadArg = (Arc<dyn IOBackend>, IOTask, SBox<HandleState>);
pub type IOThread = dyn BackgroundThread<ThreadArg, ()>;

pub fn create_io_thread(metrics: Arc<MetricsRegistry>) -> impl Fn(ThreadArg) {
  move |(backend, task, state)| {
    metrics.active_io_threads.inc();
    match task {
      IOTask::Write(handle) => handle.handle_write(&metrics, &*backend, &state),
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
) {
  if buffered.is_empty() {
    return;
  }

  let (values, waiting): (Vec<_>, Vec<_>) = buffered.drain(..).unzip();
  let Some(_token) = state.pin.try_shared() else {
    state.closed.fetch_or(true, Ordering::Release);
    return waiting.into_iter().for_each(|done| done.fulfill(Ok(())));
  };

  let result = write_exec(metrics, backend, values).map_err(|err| err.kind());
  waiting
    .into_iter()
    .for_each(|done| done.fulfill(result.map_err(Error::from)));
}

fn write_exec(
  metrics: &MetricsRegistry,
  backend: &dyn IOBackend,
  mut buffered: Vec<WriteTask>,
) -> std::io::Result<()> {
  if buffered.len() == 1 {
    let (p, buf) = &buffered[0];
    metrics.disk_write_batch.record(1.0);
    measure!(metrics.disk_write, backend.pwrite_all(buf, *p))?;
    return Ok(());
  }

  // last caller wins on duplicate pointers
  buffered.sort_by_key(|(i, _)| *i);
  buffered.reverse();
  buffered.dedup_by_key(|(i, b)| (*i, b.len()));
  buffered.reverse();

  for chunk in buffered.chunk_by(|(a_o, a_b), (b_o, _)| a_o + a_b.len() as u64 == *b_o) {
    let (offset, mut bufs): (Vec<_>, Vec<_>) =
      chunk.iter().map(|(o, b)| (*o, *b)).unzip();
    metrics.disk_write_batch.record(bufs.len() as f64);

    let offset = offset[0];
    if bufs.len() == 1 {
      measure!(metrics.disk_write, backend.pwrite_all(&bufs[0], offset))?;
      continue;
    }

    measure!(metrics.disk_write, backend.pwritev_all(&mut bufs, offset))?;
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
