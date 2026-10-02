use std::{
  cell::Cell,
  io::{Error, ErrorKind, IoSlice, Result},
  mem::{forget, replace},
  sync::{
    atomic::{fence, AtomicBool, Ordering},
    Arc,
  },
  time::Instant,
};

use crossbeam::{queue::SegQueue, utils::Backoff};

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
  state: Arc<State>,
}
impl SyncScheduler {
  pub fn new(backend: Arc<dyn IOBackend>) -> Self {
    Self {
      backend,
      sync: SyncBatch::default(),
      state: Arc::new(State::new()),
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
  let mut buffered = Vec::with_capacity(count);
  for task in guard.queue.drain_n(count) {
    buffered.push(task);
  }
  if buffered.is_empty() {
    return;
  }
  let state = guard.state.clone();
  let Some(_token) = state.try_shared() else {
    state.closed.fetch_or(true, Ordering::Relaxed);
    return buffered
      .into_iter()
      .for_each(|(_, done)| done.fulfill(Ok(())));
  };
  guard.metrics.disk_write_batch.record(buffered.len() as f64);

  buffered.sort_by_key(|((i, _), _)| *i);

  let Some(alloc) = guard.alloc.as_deref() else {
    return finish_write(guard, buffered);
  };

  // Space allocation is owned by this batching layer. Since all writes for this
  // handle are flushed here, the worker can preallocate once up to the highest
  // required offset before issuing the actual writes.
  let required = buffered
    .last()
    .map(|((o, b), _)| *o + b.len() as u64)
    .unwrap();
  let mut allocated = unsafe { alloc.get() };
  if allocated >= required {
    return finish_write(guard, buffered);
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
  let callback = create_fallocate_callback(guard, buffered, allocated);
  let (task, _) = IOTask::new_fallocate(offset, len, Some(callback));
  backend.submit(task);
}

struct BatchedWrite {
  offset: u64,
  bufs: Vec<IoSlice<'static>>,
  waiting: Vec<OneshotFulfill<Result<()>>>,
  measure: Option<Instant>,
  guard: SBox<RecursiveWrite>,
  error: Option<ErrorKind>,
}
impl BatchedWrite {
  fn new(
    offset: u64,
    bufs: Vec<IoSlice<'static>>,
    waiting: Vec<OneshotFulfill<Result<()>>>,
    guard: SBox<RecursiveWrite>,
  ) -> Self {
    Self {
      offset,
      bufs,
      waiting,
      measure: guard.metrics.disk_write.start(),
      guard,
      error: None,
    }
  }
  const fn set_error(&mut self, err: ErrorKind) {
    self.error = Some(err);
  }

  const fn get_bufs(&self) -> &[IoSlice<'static>] {
    self.bufs.as_slice()
  }

  fn bytes_len(&self) -> usize {
    self.bufs.iter().map(|v| v.len()).sum()
  }
}
impl Drop for BatchedWrite {
  fn drop(&mut self) {
    self.guard.metrics.disk_write.record(self.measure.take());
    let result = self.error.take().map(Err).unwrap_or(Ok(()));
    for done in self.waiting.drain(..) {
      done.fulfill(result.map_err(Error::from));
    }
  }
}

fn finish_write(
  guard: RecursiveWrite,
  buffered: Vec<(WriteTask, OneshotFulfill<Result<()>>)>,
) {
  debug_assert!(!buffered.is_empty());
  let backend = guard.backend.clone();
  let guard = SBox::new(guard);
  let mut buffered = buffered.into_iter();
  let ((mut start, buf), done) = buffered.next().unwrap();
  let mut last = (start, buf.len());
  let mut bufs = vec![buf];
  let mut waiting = vec![done];

  for ((offset, buf), done) in buffered {
    let (last_offset, last_len) = replace(&mut last, (offset, buf.len()));
    if last_len == buf.len() && last_offset == offset {
      let i = bufs.len() - 1;
      bufs[i] = buf;
      waiting.push(done);
      continue;
    }
    if last_len as u64 + last_offset == offset {
      bufs.push(buf);
      waiting.push(done);
      continue;
    };

    let bufs = replace(&mut bufs, vec![buf]);
    let waiting = replace(&mut waiting, vec![done]);
    let offset = replace(&mut start, offset);

    let batched = BatchedWrite::new(offset, bufs, waiting, guard.clone());
    let static_ref =
      unsafe { create_static_ref::<[IoSlice<'static>]>(batched.get_bufs()) };
    let callback = create_write_callback(batched, 0);
    let (task, _) = if static_ref.len() == 1 {
      IOTask::new_pwrite(&static_ref[0], offset, Some(callback))
    } else {
      IOTask::new_pwritev(static_ref, offset, Some(callback))
    };
    backend.submit(task);
  }

  let offset = start;
  let batched = BatchedWrite::new(offset, bufs, waiting, guard);
  let static_ref = unsafe { create_static_ref::<[IoSlice<'static>]>(batched.get_bufs()) };
  let callback = create_write_callback(batched, 1);
  let (task, _) = if static_ref.len() == 1 {
    IOTask::new_pwrite(&static_ref[0], offset, Some(callback))
  } else {
    IOTask::new_pwritev(static_ref, offset, Some(callback))
  };
  backend.submit(task);
}

fn create_write_callback(
  mut batched: BatchedWrite,
  trial: u8,
) -> impl FnOnce(&Result<usize>) {
  move |result: &Result<usize>| match result {
    Ok(c) if *c < batched.bytes_len() => retry_write(batched, trial + 1),
    Ok(_) => {}
    Err(err) => batched.set_error(err.kind()),
  }
}
fn retry_write(mut batched: BatchedWrite, trial: u8) {
  if trial >= IO_RETRY {
    return batched.set_error(ErrorKind::WriteZero);
  };

  let backend = batched.guard.backend.clone();
  let offset = batched.offset;
  let static_ref = unsafe { create_static_ref::<[IoSlice<'static>]>(batched.get_bufs()) };
  let callback = create_write_callback(batched, trial + 1);
  let (task, _) = if static_ref.len() == 1 {
    IOTask::new_pwrite(&static_ref[0], offset, Some(callback))
  } else {
    IOTask::new_pwritev(static_ref, offset, Some(callback))
  };
  backend.submit(task);
}

fn create_fallocate_callback(
  guard: RecursiveWrite,
  buffered: Vec<(WriteTask, OneshotFulfill<Result<()>>)>,
  allocated: u64,
) -> impl FnOnce(&Result<usize>) {
  move |result| {
    if let Err(err) = result {
      let kind = err.kind();
      return buffered
        .into_iter()
        .for_each(|(_, done)| done.fulfill(Err(Error::from(kind))));
    };
    unsafe { guard.alloc.as_deref().unwrap().set(allocated) };

    let state = guard.state.clone();
    let Some(_token) = state.try_shared() else {
      state.closed.fetch_or(true, Ordering::Relaxed);
      return buffered
        .into_iter()
        .for_each(|(_, done)| done.fulfill(Ok(())));
    };

    finish_write(guard, buffered);
  }
}

struct RecursiveWrite {
  queue: Arc<BatchQueue<WriteTask, Result<()>>>,
  state: Arc<State>,
  backend: Arc<dyn IOBackend>,
  metrics: Arc<MetricsRegistry>,
  alloc: Option<Arc<AllocState>>,
}
impl RecursiveWrite {
  const fn new(
    queue: Arc<BatchQueue<WriteTask, Result<()>>>,
    state: Arc<State>,
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

const EXTENT_SIZE: u64 = 1 << 20;

struct RecursiveSync {
  queue: Arc<BatchQueue<(), Result<()>>>,
  state: Arc<State>,
  backend: Arc<dyn IOBackend>,
  metrics: Arc<MetricsRegistry>,
}
impl RecursiveSync {
  const fn new(
    queue: Arc<BatchQueue<(), Result<()>>>,
    state: Arc<State>,
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
    return waiting.into_iter().for_each(|done| done.fulfill(Ok(())));
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

struct State {
  /**
   * Pin to protect file I/O from truncate.
   */
  pin: ExclusivePin,
  /**
   * Flag to check for file existence a little faster.
   */
  closed: AtomicBool,
}
impl State {
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
