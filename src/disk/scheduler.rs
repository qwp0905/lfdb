use std::{
  cell::Cell,
  io::{Error, ErrorKind, IoSlice, Result},
  sync::{
    atomic::{fence, AtomicBool, AtomicU16, Ordering},
    Arc,
  },
  time::Instant,
};

use crossbeam::{atomic::AtomicCell, queue::SegQueue};

use super::{max_iov, IOBackend, IOTask, IO_RETRY};
use crate::{
  background::{oneshot, Callback, Oneshot, OneshotFulfill},
  metrics::MetricsRegistry,
  utils::{create_static_ref, ExclusivePin, ExclusiveToken, SharedToken},
};

type WriteTask = (u64, IoSlice<'static>);

pub type WriteBatch = Arc<BatchQueue<WriteTask, Result<()>>>;
pub type SyncBatch = Arc<BatchQueue<(), Result<()>>>;

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
}

struct WriteBatchArg {
  state: Arc<HandleState>,
  backend: Arc<dyn IOBackend>,
  metrics: Arc<MetricsRegistry>,
  alloc: Option<Arc<AllocState>>,
}
impl Clone for WriteBatchArg {
  fn clone(&self) -> Self {
    Self {
      state: self.state.clone(),
      backend: self.backend.clone(),
      metrics: self.metrics.clone(),
      alloc: self.alloc.clone(),
    }
  }
}

struct UnsafeVec<T> {
  ptr: *mut T,
  len: usize,
  cap: usize,
}
impl<T> UnsafeVec<T> {
  fn new(v: Vec<T>) -> Self {
    let (ptr, len, cap) = Vec::into_raw_parts(v);
    Self { ptr, len, cap }
  }
  unsafe fn take(&self) -> Vec<T> {
    unsafe { Vec::from_raw_parts(self.ptr, self.len, self.cap) }
  }
}
impl<T> Clone for UnsafeVec<T> {
  fn clone(&self) -> Self {
    Self {
      ptr: self.ptr,
      len: self.len,
      cap: self.cap,
    }
  }
}
unsafe impl<T: Send> Send for UnsafeVec<T> {}

struct BatchWriteResult {
  count: AtomicU16,
  error: AtomicCell<Option<ErrorKind>>,
}
impl BatchWriteResult {
  const fn new() -> Self {
    Self {
      count: AtomicU16::new(1),
      error: AtomicCell::new(None),
    }
  }

  #[must_use]
  fn resolve_with(&self, waiting: UnsafeVec<OneshotFulfill<Result<()>>>) -> bool {
    if self.count.fetch_sub(1, Ordering::AcqRel) > 1 {
      return false;
    }

    let result = self.error.load().map(Err).unwrap_or(Ok(()));
    for done in unsafe { waiting.take() } {
      done.fulfill(result.map_err(Error::from));
    }
    true
  }

  fn set_error(&self, error: ErrorKind) {
    let _ = self.error.compare_exchange(None, Some(error));
  }

  fn increase_count(&self) {
    self.count.fetch_add(1, Ordering::Release);
  }
}

impl BatchQueue<WriteTask, Result<()>> {
  fn recursive_write(self: &Arc<Self>, arg: WriteBatchArg) {
    if self.try_release() {
      return;
    }
    self.drain_write(arg);
  }

  fn finish_write(
    self: &Arc<Self>,
    arg: &WriteBatchArg,
    values: Vec<WriteTask>,
    waiting: Vec<OneshotFulfill<Result<()>>>,
  ) {
    let waiting = UnsafeVec::new(waiting);
    let mut failed: Option<Vec<_>> = None;
    let result = Arc::new(BatchWriteResult::new());
    for chunk in values.chunk_by(|(a_o, a_b), (b_o, _)| a_o + a_b.len() as u64 == *b_o) {
      result.increase_count();
      let mut bufs = Vec::with_capacity(chunk.len());
      let (offset, _) = chunk.first().copied().unwrap();
      for (_, buf) in chunk {
        bufs.push(*buf);
      }

      let static_ref = unsafe { create_static_ref::<[IoSlice<'static>]>(&*bufs) };
      let (task, done) = if bufs.len() == 1 {
        IOTask::new_pwrite(&static_ref[0], offset)
      } else {
        IOTask::new_pwritev(static_ref, offset)
      };

      let measure = arg.metrics.disk_write.start();
      let callback = self.create_callback(
        arg,
        bufs,
        offset,
        waiting.clone(),
        result.clone(),
        measure,
        0,
      );
      if let Err(err) = arg.backend.submit(task) {
        failed.get_or_insert_default().push((err, callback));
        continue;
      };
      done.must_call(callback);
    }

    for (err, callback) in failed.into_iter().flatten() {
      callback.call(&Err(err))
    }

    if result.resolve_with(waiting) {
      self.recursive_write(arg.clone());
    }
  }

  fn create_callback(
    self: &Arc<Self>,
    arg: &WriteBatchArg,
    bufs: Vec<IoSlice<'static>>,
    offset: u64,
    waiting: UnsafeVec<OneshotFulfill<Result<()>>>,
    result: Arc<BatchWriteResult>,
    measurement: Option<Instant>,
    trial: u8,
  ) -> Callback<Result<usize>> {
    let bytes = bufs.iter().map(|v| v.len()).sum::<usize>();
    let queue = self.clone();
    let arg = arg.clone();
    Callback::new(move |r: &Result<usize>| {
      match r {
        Ok(c) if *c < bytes => {
          return queue.retry_write(
            arg,
            trial,
            offset,
            bufs,
            waiting,
            result,
            measurement,
          )
        }
        Ok(_) => {}
        Err(err) => result.set_error(err.kind()),
      };
      arg.metrics.disk_write.record(measurement);
      if result.resolve_with(waiting) {
        queue.recursive_write(arg);
      };
    })
  }
  fn retry_write(
    self: &Arc<Self>,
    arg: WriteBatchArg,
    trial: u8,
    offset: u64,
    bufs: Vec<IoSlice<'static>>,
    waiting: UnsafeVec<OneshotFulfill<Result<()>>>,
    result: Arc<BatchWriteResult>,
    measurement: Option<Instant>,
  ) {
    if trial >= IO_RETRY {
      arg.metrics.disk_write.record(measurement);
      result.set_error(ErrorKind::WriteZero);
      if !result.resolve_with(waiting) {
        return;
      };
      return self.recursive_write(arg);
    };

    let static_ref = unsafe { create_static_ref::<[IoSlice<'static>]>(&bufs) };
    let (task, done) = if bufs.len() == 1 {
      IOTask::new_pwrite(&static_ref[0], offset)
    } else {
      IOTask::new_pwritev(static_ref, offset)
    };

    let callback =
      self.create_callback(&arg, bufs, offset, waiting, result, measurement, trial + 1);
    if let Err(err) = arg.backend.submit(task) {
      return callback.call(&Err(err));
    }
    done.must_call(callback);
  }
  fn drain_write(self: &Arc<Self>, arg: WriteBatchArg) {
    let count = max_iov();
    let mut values = Vec::with_capacity(count);
    let mut waiting = Vec::with_capacity(count);
    for (task, done) in (0..count).map_while(|_| self.queue.pop()) {
      values.push(task);
      waiting.push(done);
    }

    if waiting.is_empty() {
      return self.recursive_write(arg);
    }

    let Some(token) = arg.state.pin.try_shared() else {
      arg.state.closed.fetch_or(true, Ordering::Relaxed);
      waiting.into_iter().for_each(|done| done.fulfill(Ok(())));
      return self.recursive_write(arg);
    };
    arg.metrics.disk_write_batch.record(waiting.len() as f64);

    if values.len() > 1 {
      values.sort_by_key(|(i, _)| *i);
      values.reverse();
      values.dedup_by_key(|(i, b)| (*i, b.len()));
      values.reverse();
    }

    let Some(alloc) = arg.alloc.as_deref() else {
      return self.finish_write(&arg, values, waiting);
    };

    // Space allocation is owned by this batching layer. Since all writes for this
    // handle are flushed here, the worker can preallocate once up to the highest
    // required offset before issuing the actual writes.
    let required = values.last().map(|(o, b)| *o + b.len() as u64).unwrap();
    let (done, allocated) = match alloc_if_needed(required, alloc, &*arg.backend) {
      Ok(Some(v)) => v,
      Ok(None) => return self.finish_write(&arg, values, waiting),
      Err(err) => {
        drop(token);
        waiting
          .into_iter()
          .for_each(|done| done.fulfill(Err(Error::from(err.kind()))));
        return self.recursive_write(arg);
      }
    };

    drop(token);
    let queue = self.clone();
    let callback = Callback::new(move |r: &Result<usize>| {
      if let Err(err) = r {
        waiting
          .into_iter()
          .for_each(|done| done.fulfill(Err(Error::from(err.kind()))));
        return queue.recursive_write(arg);
      };

      let Some(_token) = arg.state.pin.try_shared() else {
        arg.state.closed.fetch_or(true, Ordering::Relaxed);
        waiting.into_iter().for_each(|done| done.fulfill(Ok(())));
        return queue.recursive_write(arg);
      };
      arg.alloc.as_deref().unwrap().set(allocated);
      queue.finish_write(&arg, values, waiting);
    });
    if let Err(err) = done.add_callback(callback) {
      err.call(&done.wait().unwrap());
    };
  }

  pub fn publish_write(
    self: &Arc<Self>,
    state: &Arc<HandleState>,
    backend: &Arc<dyn IOBackend>,
    metrics: &Arc<MetricsRegistry>,
    alloc: &Option<Arc<AllocState>>,
    buf: &'static [u8],
    offset: u64,
  ) -> Oneshot<Result<()>> {
    let (o, occupied) = self.push_and_compete((offset, IoSlice::new(buf)));
    if occupied {
      let arg = WriteBatchArg {
        state: state.clone(),
        backend: backend.clone(),
        metrics: metrics.clone(),
        alloc: alloc.clone(),
      };
      self.drain_write(arg);
    }
    o
  }
}
impl BatchQueue<(), Result<()>> {
  fn recursive_sync(
    self: &Arc<Self>,
    state: Arc<HandleState>,
    backend: Arc<dyn IOBackend>,
    metrics: Arc<MetricsRegistry>,
  ) {
    if self.try_release() {
      return;
    }
    self.drain_sync(state, backend, metrics);
  }
  fn drain_sync(
    self: &Arc<Self>,
    state: Arc<HandleState>,
    backend: Arc<dyn IOBackend>,
    metrics: Arc<MetricsRegistry>,
  ) {
    const COUNT: usize = 512;
    let mut waiting = Vec::with_capacity(COUNT);
    for (_, done) in (0..COUNT).map_while(|_| self.queue.pop()) {
      waiting.push(done);
    }

    if waiting.is_empty() {
      return self.recursive_sync(state, backend, metrics);
    }

    let Some(token) = state.pin.try_shared() else {
      state.closed.fetch_or(true, Ordering::Relaxed);
      waiting.into_iter().for_each(|done| done.fulfill(Ok(())));
      return self.recursive_sync(state, backend, metrics);
    };

    metrics.disk_sync_batch.record(waiting.len() as f64);
    let (task, done) = IOTask::new_fdatasync();
    if let Err(err) = backend.submit(task) {
      drop(token);
      waiting
        .into_iter()
        .for_each(|done| done.fulfill(Err(Error::from(err.kind()))));
      return self.recursive_sync(state, backend, metrics);
    }

    drop(token);
    let queue = self.clone();
    let callback = Callback::new(move |r: &Result<usize>| {
      let result = r.as_ref().map(|_| ()).map_err(|err| err.kind());
      for done in waiting {
        done.fulfill(result.map_err(Error::from));
      }
      queue.recursive_sync(state, backend, metrics);
    });
    if let Err(err) = done.add_callback(callback) {
      err.call(&done.wait().unwrap());
    };
  }
  pub fn publish_sync(
    self: &Arc<Self>,
    state: &Arc<HandleState>,
    backend: &Arc<dyn IOBackend>,
    metrics: &Arc<MetricsRegistry>,
  ) -> Oneshot<Result<()>> {
    let (o, occupied) = self.push_and_compete(());
    if occupied {
      let state = state.clone();
      let backend = backend.clone();
      let metrics = metrics.clone();
      self.drain_sync(state, backend, metrics);
    }
    o
  }
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
  pub const fn get(&self) -> u64 {
    self.0.get()
  }
  pub fn set(&self, allocated: u64) {
    self.0.set(allocated);
  }
}
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
    self.closed.load(Ordering::Relaxed)
  }

  pub fn try_shared(&self) -> Option<SharedToken<'_>> {
    self.pin.try_shared()
  }
  pub fn try_exclusive(&self) -> Option<ExclusiveToken<'_>> {
    self.pin.try_exclusive()
  }
}

// Preallocate in coarse chunks so the filesystem can keep nearby writes in a
// more local extent instead of allocating space block by block. 1 MiB is a
// simple default chunk size, not a carefully tuned boundary.
const EXTENT: u64 = 1 << 20;
fn alloc_if_needed(
  required: u64,
  alloc: &AllocState,
  backend: &dyn IOBackend,
) -> Result<Option<(Oneshot<Result<usize>>, u64)>> {
  let mut allocated = alloc.get();
  if allocated >= required {
    return Ok(None);
  }
  while required >= allocated {
    allocated += EXTENT;
  }
  let done = backend.submit_fallocate(alloc.get(), allocated - alloc.get())?;
  Ok(Some((done, allocated)))
}
