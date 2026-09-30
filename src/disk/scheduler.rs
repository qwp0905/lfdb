use std::{
  cell::Cell,
  io::{Error, ErrorKind, IoSlice, Result},
  mem::forget,
  sync::{
    atomic::{fence, AtomicBool, Ordering},
    Arc,
  },
  time::Instant,
};

use crossbeam::{atomic::AtomicCell, queue::SegQueue, utils::Backoff};

use super::{max_iov, IOBackend, IOTask, IO_RETRY};
use crate::{
  background::{oneshot, Oneshot, OneshotFulfill},
  metrics::MetricsRegistry,
  utils::{create_static_ref, ExclusivePin, ExclusiveToken, SBox, SharedToken},
};

type WriteTask = (u64, IoSlice<'static>);

type WriteBatch = Arc<BatchQueue<WriteTask, Result<()>>>;
type SyncBatch = Arc<BatchQueue<(), Result<()>>>;

pub struct SyncScheduler {
  backend: Arc<dyn IOBackend>,
  sync: SyncBatch,
  state: Arc<HandleState>,
}
impl SyncScheduler {
  pub fn new(backend: Arc<dyn IOBackend>) -> Self {
    Self {
      backend,
      sync: SyncBatch::default(),
      state: Arc::new(HandleState::new()),
    }
  }

  pub fn publish(&self, metrics: &Arc<MetricsRegistry>) -> Oneshot<Result<()>> {
    let (o, occupied) = self.sync.push_and_compete(());
    if occupied {
      let guard = RecursiveSync::new(
        self.sync.clone(),
        self.state.clone(),
        self.backend.clone(),
        metrics.clone(),
      );
      drain_sync(guard);
    }
    o
  }

  pub fn is_closed(&self) -> bool {
    self.state.is_closed()
  }

  pub fn backend(&self) -> &dyn IOBackend {
    &*self.backend
  }
}

pub struct IOScheduler {
  inner: SyncScheduler,
  write: WriteBatch,
  alloc: Option<Arc<AllocState>>,
}
impl IOScheduler {
  pub fn new(backend: Arc<dyn IOBackend>, alloc: Option<AllocState>) -> Self {
    Self {
      inner: SyncScheduler::new(backend),
      write: WriteBatch::default(),
      alloc: alloc.map(Arc::new),
    }
  }
  pub fn backend(&self) -> &dyn IOBackend {
    &*self.inner.backend
  }
  pub fn pin_state(&self) -> Option<SharedToken<'_>> {
    self.inner.state.try_shared()
  }

  pub fn is_closed(&self) -> bool {
    self.inner.is_closed()
  }
  pub fn close(&self) {
    let backoff = Backoff::new();
    while self.inner.state.try_exclusive().map(forget).is_none() {
      backoff.snooze();
    }
  }

  pub fn publish_write(
    &self,
    buf: &'static [u8],
    offset: u64,
    metrics: &Arc<MetricsRegistry>,
  ) -> Oneshot<Result<()>> {
    let (o, occupied) = self.write.push_and_compete((offset, IoSlice::new(buf)));
    if occupied {
      let guard = RecursiveWrite::new(
        self.write.clone(),
        self.inner.state.clone(),
        self.inner.backend.clone(),
        metrics.clone(),
        self.alloc.clone(),
      );
      drain_write(guard);
    };
    o
  }
  pub fn publish_sync(&self, metrics: &Arc<MetricsRegistry>) -> Oneshot<Result<()>> {
    self.inner.publish(metrics)
  }
}

pub struct BatchQueue<T, R> {
  queue: SegQueue<(T, OneshotFulfill<R>)>,
  occupied: AtomicBool,
}
impl<T, R> Default for BatchQueue<T, R> {
  fn default() -> Self {
    Self::new()
  }
}
impl<T, R> BatchQueue<T, R> {
  const fn new() -> Self {
    Self {
      queue: SegQueue::new(),
      occupied: AtomicBool::new(false),
    }
  }
  fn try_release(&self) -> bool {
    self.occupied.fetch_and(false, Ordering::Release);
    if self.queue.is_empty() {
      return true;
    }
    if self.occupied.fetch_or(true, Ordering::Relaxed) {
      return true;
    }
    fence(Ordering::Acquire);
    false
  }

  fn push_and_compete(&self, v: T) -> (Oneshot<R>, bool) {
    let (o, f) = oneshot();
    self.queue.push((v, f));
    if self.occupied.fetch_or(true, Ordering::Relaxed) {
      return (o, false);
    }
    fence(Ordering::Acquire);
    (o, true)
  }

  fn drain_n(&self, count: usize) -> impl Iterator<Item = (T, OneshotFulfill<R>)> + '_ {
    (0..count).map_while(|_| self.queue.pop())
  }
}

fn drain_write(guard: RecursiveWrite) {
  let count = max_iov();
  let mut values = Vec::with_capacity(count);
  let mut waiting = Vec::with_capacity(count);
  for (task, done) in guard.queue.drain_n(count) {
    values.push(task);
    waiting.push(done);
  }
  if waiting.is_empty() {
    return;
  }
  let state = guard.state.clone();
  let Some(_token) = state.try_shared() else {
    state.closed.fetch_or(true, Ordering::Relaxed);
    return waiting.into_iter().for_each(|done| done.fulfill(Ok(())));
  };
  guard.metrics.disk_write_batch.record(waiting.len() as f64);

  if values.len() > 1 {
    values.sort_by_key(|(i, _)| *i);
    values.reverse();
    values.dedup_by_key(|(i, b)| (*i, b.len()));
    values.reverse();
  }

  let Some(alloc) = guard.alloc.as_deref() else {
    return finish_write(guard, values, waiting);
  };

  // Space allocation is owned by this batching layer. Since all writes for this
  // handle are flushed here, the worker can preallocate once up to the highest
  // required offset before issuing the actual writes.
  let required = values.last().map(|(o, b)| *o + b.len() as u64).unwrap();
  let mut allocated = unsafe { alloc.get() };
  if allocated >= required {
    return finish_write(guard, values, waiting);
  }

  let current = allocated;
  // Preallocate in coarse chunks so the filesystem can keep nearby writes in a
  // more local extent instead of allocating space block by block. 1 MiB is a
  // simple default chunk size, not a carefully tuned boundary.
  while required >= allocated {
    allocated += EXTENT_SIZE;
  }
  let (offset, len) = (current, allocated - current);

  let backend = guard.backend.clone();
  let callback = create_fallocate_callback(guard, values, waiting, allocated);
  let (task, _) = IOTask::new_fallocate(offset, len, Some(callback));
  backend.submit(task);
}

fn finish_write(
  guard: RecursiveWrite,
  values: Vec<WriteTask>,
  waiting: Vec<OneshotFulfill<Result<()>>>,
) {
  let mut offsets = Vec::with_capacity(values.len());
  let mut buffers = Vec::with_capacity(values.len());
  for (offset, buf) in values {
    offsets.push((offset, buf.len() as u64));
    buffers.push(buf);
  }
  let result = SBox::new(BatchWriteResult::new(waiting, buffers, guard));

  let mut index = 0;
  for chunk in offsets.chunk_by(|(a_o, a_l), (b_o, _)| a_o + a_l == *b_o) {
    let (offset, _) = chunk.first().copied().unwrap();
    let start = index;
    let end = index + chunk.len();
    index += chunk.len();

    let static_ref =
      unsafe { create_static_ref::<[IoSlice<'static>]>(result.range_buffer(start, end)) };
    let measure = result.guard.metrics.disk_write.start();
    let callback =
      create_write_callback((start, end), offset, result.clone(), measure, 1);

    let (task, _) = if static_ref.len() == 1 {
      IOTask::new_pwrite(&static_ref[0], offset, Some(callback))
    } else {
      IOTask::new_pwritev(static_ref, offset, Some(callback))
    };
    result.guard.backend.submit(task);
  }
}

fn create_write_callback(
  (start, end): (usize, usize),
  offset: u64,
  result: SBox<BatchWriteResult>,
  measurement: Option<Instant>,
  trial: u8,
) -> impl FnOnce(&Result<usize>) {
  let bytes = result
    .range_buffer(start, end)
    .iter()
    .map(|v| v.len())
    .sum::<usize>();
  move |r: &Result<usize>| {
    match r {
      Ok(c) if *c < bytes => {
        return retry_write(trial, offset, (start, end), result, measurement)
      }
      Ok(_) => {}
      Err(err) => result.set_error(err.kind()),
    };
    result.guard.metrics.disk_write.record(measurement);
  }
}
fn retry_write(
  trial: u8,
  offset: u64,
  (start, end): (usize, usize),
  result: SBox<BatchWriteResult>,
  measurement: Option<Instant>,
) {
  if trial >= IO_RETRY {
    result.guard.metrics.disk_write.record(measurement);
    result.set_error(ErrorKind::WriteZero);
    return;
  };

  let static_ref =
    unsafe { create_static_ref::<[IoSlice<'static>]>(result.range_buffer(start, end)) };
  let callback =
    create_write_callback((start, end), offset, result.clone(), measurement, trial + 1);
  let (task, _) = if static_ref.len() == 1 {
    IOTask::new_pwrite(&static_ref[0], offset, Some(callback))
  } else {
    IOTask::new_pwritev(static_ref, offset, Some(callback))
  };
  result.guard.backend.submit(task);
}

fn create_fallocate_callback(
  guard: RecursiveWrite,
  values: Vec<WriteTask>,
  waiting: Vec<OneshotFulfill<Result<()>>>,
  allocated: u64,
) -> impl FnOnce(&Result<usize>) {
  move |result| {
    if let Err(err) = result {
      waiting
        .into_iter()
        .for_each(|done| done.fulfill(Err(Error::from(err.kind()))));
      return;
    };
    unsafe { guard.alloc.as_deref().unwrap().set(allocated) };

    let state = guard.state.clone();
    let Some(_token) = state.try_shared() else {
      state.closed.fetch_or(true, Ordering::Relaxed);
      return waiting.into_iter().for_each(|done| done.fulfill(Ok(())));
    };

    finish_write(guard, values, waiting);
  }
}

struct RecursiveWrite {
  queue: Arc<BatchQueue<WriteTask, Result<()>>>,
  state: Arc<HandleState>,
  backend: Arc<dyn IOBackend>,
  metrics: Arc<MetricsRegistry>,
  alloc: Option<Arc<AllocState>>,
}
impl RecursiveWrite {
  const fn new(
    queue: Arc<BatchQueue<WriteTask, Result<()>>>,
    state: Arc<HandleState>,
    backend: Arc<dyn IOBackend>,
    metrics: Arc<MetricsRegistry>,
    alloc: Option<Arc<AllocState>>,
  ) -> Self {
    Self {
      queue,
      state,
      backend,
      metrics,
      alloc,
    }
  }
}
impl Drop for RecursiveWrite {
  fn drop(&mut self) {
    if self.queue.try_release() {
      return;
    }

    drain_write(Self::new(
      self.queue.clone(),
      self.state.clone(),
      self.backend.clone(),
      self.metrics.clone(),
      self.alloc.clone(),
    ));
  }
}

struct BatchWriteResult {
  error: AtomicCell<Option<ErrorKind>>,
  waiting: Vec<OneshotFulfill<Result<()>>>,
  buffers: Vec<IoSlice<'static>>,
  guard: RecursiveWrite,
}
impl BatchWriteResult {
  const fn new(
    waiting: Vec<OneshotFulfill<Result<()>>>,
    buffers: Vec<IoSlice<'static>>,
    guard: RecursiveWrite,
  ) -> Self {
    Self {
      error: AtomicCell::new(None),
      waiting,
      buffers,
      guard,
    }
  }

  fn range_buffer(&self, start: usize, end: usize) -> &[IoSlice<'static>] {
    &self.buffers[start..end]
  }

  fn set_error(&self, error: ErrorKind) {
    let _ = self.error.compare_exchange(None, Some(error));
  }
}
impl Drop for BatchWriteResult {
  fn drop(&mut self) {
    let result = self.error.load().map(Err).unwrap_or(Ok(()));
    for done in self.waiting.drain(..) {
      done.fulfill(result.map_err(Error::from));
    }
  }
}

const EXTENT_SIZE: u64 = 1 << 20;

struct RecursiveSync {
  queue: Arc<BatchQueue<(), Result<()>>>,
  state: Arc<HandleState>,
  backend: Arc<dyn IOBackend>,
  metrics: Arc<MetricsRegistry>,
}
impl RecursiveSync {
  const fn new(
    queue: Arc<BatchQueue<(), Result<()>>>,
    state: Arc<HandleState>,
    backend: Arc<dyn IOBackend>,
    metrics: Arc<MetricsRegistry>,
  ) -> Self {
    Self {
      queue,
      state,
      backend,
      metrics,
    }
  }
}
impl Drop for RecursiveSync {
  fn drop(&mut self) {
    if self.queue.try_release() {
      return;
    }
    drain_sync(Self::new(
      self.queue.clone(),
      self.state.clone(),
      self.backend.clone(),
      self.metrics.clone(),
    ));
  }
}

fn drain_sync(guard: RecursiveSync) {
  const COUNT: usize = 512;
  let mut waiting = Vec::with_capacity(COUNT);
  for (_, done) in guard.queue.drain_n(COUNT) {
    waiting.push(done);
  }

  if waiting.is_empty() {
    return;
  }

  let state = guard.state.clone();
  let Some(_token) = state.pin.try_shared() else {
    state.closed.fetch_or(true, Ordering::Relaxed);
    waiting.into_iter().for_each(|done| done.fulfill(Ok(())));
    return;
  };

  guard.metrics.disk_sync_batch.record(waiting.len() as f64);
  let backend = guard.backend.clone();
  let callback = move |r: &Result<usize>| {
    let _guard = guard;
    let result = r.as_ref().map(|_| ()).map_err(|err| err.kind());
    for done in waiting {
      done.fulfill(result.map_err(Error::from));
    }
  };
  let (task, _) = IOTask::new_fdatasync(Some(callback));
  backend.submit(task);
}

/**
 * Tracks the file size already covered by preallocation.
 *
 * `AllocState` uses `Cell` because it is only accessed by the single worker that
 * owns the corresponding write flush pass. The value is stored in an
 * `Arc` and can move across threads, but `BatchExecutor` serializes
 * execution so the state is logically single-threaded.
 */
pub struct AllocState(Cell<u64>);
impl AllocState {
  pub const fn new(allocated: u64) -> Self {
    Self(Cell::new(allocated))
  }
  pub const unsafe fn get(&self) -> u64 {
    self.0.get()
  }
  pub unsafe fn set(&self, allocated: u64) {
    self.0.set(allocated);
  }
}
unsafe impl Send for AllocState {}
unsafe impl Sync for AllocState {}

struct HandleState {
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
  const fn new() -> Self {
    Self {
      pin: ExclusivePin::new(),
      closed: AtomicBool::new(false),
    }
  }

  fn is_closed(&self) -> bool {
    self.closed.load(Ordering::Relaxed)
  }

  fn try_shared(&self) -> Option<SharedToken<'_>> {
    self.pin.try_shared()
  }
  fn try_exclusive(&self) -> Option<ExclusiveToken<'_>> {
    self.pin.try_exclusive()
  }
}
