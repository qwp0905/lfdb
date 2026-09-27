use std::{
  cell::Cell,
  io::{Error, IoSlice, Result},
  sync::{
    atomic::{fence, AtomicBool, AtomicU32, Ordering},
    Arc,
  },
};

use crossbeam::queue::SegQueue;

use super::{max_iov, IOBackend, IOTask};
use crate::{
  background::{oneshot, Callback, Oneshot, OneshotFulfill},
  metrics::MetricsRegistry,
  utils::{create_static_ref, ExclusivePin, ExclusiveToken, SharedToken},
};

type WriteTask = (u64, IoSlice<'static>);

struct UnsafeVec<T>(*mut T, usize, usize);
impl<T> UnsafeVec<T> {
  fn new(v: Vec<T>) -> Self {
    let (p, l, c) = v.into_raw_parts();
    Self(p, l, c)
  }
  unsafe fn take(self) -> Vec<T> {
    let Self(p, l, c) = self;
    unsafe { Vec::from_raw_parts(p, l, c) }
  }
}
impl<T> Clone for UnsafeVec<T> {
  fn clone(&self) -> Self {
    Self(self.0, self.1, self.2)
  }
}
unsafe impl<T: Send> Send for UnsafeVec<T> {}

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
impl BatchQueue<WriteTask, Result<()>> {
  fn recursive_write(
    self: &Arc<Self>,
    state: Arc<HandleState>,
    backend: Arc<dyn IOBackend>,
    metrics: Arc<MetricsRegistry>,
    alloc: Option<Arc<AllocState>>,
  ) {
    if self.try_release() {
      return;
    }
    self.drain_write(state, backend, metrics, alloc);
  }
  fn finish_write(
    self: &Arc<Self>,
    state: &Arc<HandleState>,
    backend: &Arc<dyn IOBackend>,
    metrics: &Arc<MetricsRegistry>,
    alloc: &Option<Arc<AllocState>>,
    values: Vec<WriteTask>,
    waiting: Vec<OneshotFulfill<Result<()>>>,
  ) {
    let mut tasks = Vec::new();
    let waiting = UnsafeVec::new(waiting);
    let count = Arc::new(AtomicU32::new(0));
    for chunk in values.chunk_by(|(a_o, a_b), (b_o, _)| a_o + a_b.len() as u64 == *b_o) {
      count.fetch_add(1, Ordering::Relaxed);
      let (offset, bufs): (Vec<_>, Vec<_>) = chunk.iter().map(|(o, b)| (*o, *b)).unzip();
      let offset = offset[0];
      let static_ref = unsafe { create_static_ref::<[IoSlice<'static>]>(&bufs) };
      let (task, done) = if bufs.len() == 1 {
        IOTask::new_pwrite(&static_ref[0], offset)
      } else {
        IOTask::new_pwritev(static_ref, offset)
      };

      tasks.push((task, done, bufs));
    }

    for (task, done, bufs) in tasks {
      let state = state.clone();
      let backend = backend.clone();
      let metrics = metrics.clone();
      let alloc = alloc.clone();

      if let Err(err) = backend.submit(task) {
        for done in unsafe { waiting.take() } {
          done.fulfill(Err(Error::from(err.kind())));
        }
        return self.recursive_write(state, backend, metrics, alloc);
      }
      let count = count.clone();
      let waiting = waiting.clone();
      let queue = self.clone();
      let callback = Callback::new(move |result: &Result<usize>| {
        let _bufs = bufs;
        if count.fetch_sub(1, Ordering::Relaxed) > 1 {
          return;
        }
        let result = result.as_ref().map(|_| ()).map_err(|err| err.kind());
        for done in unsafe { waiting.take() } {
          done.fulfill(result.map_err(Error::from));
        }
        queue.recursive_write(state, backend, metrics, alloc);
      });

      if let Err(err) = done.add_callback(callback) {
        err.call(&done.wait().unwrap());
      };
    }
  }
  fn drain_write(
    self: &Arc<Self>,
    state: Arc<HandleState>,
    backend: Arc<dyn IOBackend>,
    metrics: Arc<MetricsRegistry>,
    alloc: Option<Arc<AllocState>>,
  ) {
    let count = max_iov();
    let mut waiting = Vec::with_capacity(count);
    let mut values = Vec::with_capacity(count);
    for (v, done) in (0..count).map_while(|_| self.queue.pop()) {
      values.push(v);
      waiting.push(done);
    }

    if waiting.is_empty() {
      return self.recursive_write(state, backend, metrics, alloc);
    }

    let Some(token) = state.pin.try_shared() else {
      state.closed.fetch_or(true, Ordering::Relaxed);
      waiting.into_iter().for_each(|done| done.fulfill(Ok(())));
      return self.recursive_write(state, backend, metrics, alloc);
    };
    metrics.disk_write_batch.record(values.len() as f64);

    if values.len() > 1 {
      values.sort_by_key(|(i, _)| *i);
      values.reverse();
      values.dedup_by_key(|(i, b)| (*i, b.len()));
      values.reverse();
    }

    let Some(a) = alloc.as_deref() else {
      return self.finish_write(&state, &backend, &metrics, &alloc, values, waiting);
    };

    // Space allocation is owned by this batching layer. Since all writes for this
    // handle are flushed here, the worker can preallocate once up to the highest
    // required offset before issuing the actual writes.
    let required = values.last().map(|(o, b)| *o + b.len() as u64).unwrap();
    let (done, allocated) = match alloc_if_needed(required, a, &*backend) {
      Ok(Some(v)) => v,
      Ok(None) => {
        return self.finish_write(&state, &backend, &metrics, &alloc, values, waiting)
      }
      Err(err) => {
        drop(token);
        waiting
          .into_iter()
          .for_each(|done| done.fulfill(Err(Error::from(err.kind()))));
        return self.recursive_write(state, backend, metrics, alloc);
      }
    };

    drop(token);
    let queue = self.clone();
    let callback = Callback::new(move |r: &Result<usize>| {
      if let Err(err) = r {
        waiting
          .into_iter()
          .for_each(|done| done.fulfill(Err(Error::from(err.kind()))));
        return queue.recursive_write(state, backend, metrics, alloc);
      };

      let Some(_token) = state.pin.try_shared() else {
        state.closed.fetch_or(true, Ordering::Relaxed);
        waiting.into_iter().for_each(|done| done.fulfill(Ok(())));
        return queue.recursive_write(state, backend, metrics, alloc);
      };
      alloc.as_deref().unwrap().set(allocated);
      queue.finish_write(&state, &backend, &metrics, &alloc, values, waiting);
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
      let state = state.clone();
      let backend = backend.clone();
      let metrics = metrics.clone();
      let alloc = alloc.clone();
      self.drain_write(state, backend, metrics, alloc);
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
