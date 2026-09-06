use std::{
  cell::Cell,
  io::{Error, IoSlice, Result},
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
};

use super::{max_iov, IOBackend};
use crate::{
  background::{oneshot, BatchExecutor, Oneshot, OneshotFulfill, ThreadPool},
  measure,
  metrics::MetricsRegistry,
  utils::{ExclusivePin, ExclusiveToken, SharedToken},
};

type WriteTask = (u64, IoSlice<'static>);
type IOTask<T, R> = (T, OneshotFulfill<Result<R>>);

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
    self.closed.load(Ordering::Acquire)
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
  if buffered.len() > 1 {
    buffered.sort_by_key(|(i, _)| *i);
    buffered.reverse();
    buffered.dedup_by_key(|(i, b)| (*i, b.len()));
    buffered.reverse();
  }

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
      measure!(metrics.disk_write, backend.pwrite_exact(&bufs[0], offset))?;
      continue;
    }

    measure!(metrics.disk_write, backend.pwritev_exact(&bufs, offset))?;
  }
  Ok(())
}

pub struct WriteScheduler(BatchExecutor<IOTask<WriteTask, ()>>);
impl WriteScheduler {
  const MAX_FLUSH_COUNT: usize = max_iov();
  pub fn new(
    pool: Arc<ThreadPool>,
    state: Arc<HandleState>,
    backend: Arc<dyn IOBackend>,
    metrics: Arc<MetricsRegistry>,
    alloc: Option<AllocState>,
  ) -> Self {
    let executor = BatchExecutor::new(
      pool,
      Self::handle(state, backend, metrics, alloc),
      Self::MAX_FLUSH_COUNT,
    );
    Self(executor)
  }

  pub fn schedule(&self, buf: &'static [u8], offset: u64) -> Oneshot<Result<()>> {
    let (o, f) = oneshot();
    self.0.dispatch(((offset, IoSlice::new(buf)), f));
    o
  }

  const fn handle(
    state: Arc<HandleState>,
    backend: Arc<dyn IOBackend>,
    metrics: Arc<MetricsRegistry>,
    alloc: Option<AllocState>,
  ) -> impl FnMut(Vec<IOTask<WriteTask, ()>>) {
    move |buffered| {
      let Some(_token) = state.pin.try_shared() else {
        // If truncate/remove owns the handle exclusively, this request no longer has a
        // meaningful file to operate on. Mark the handle closed and complete queued
        // waiters as successful no-ops.
        state.closed.fetch_or(true, Ordering::Release);
        return buffered
          .into_iter()
          .for_each(|(_, done)| done.fulfill(Ok(())));
      };

      metrics.disk_write_batch.record(buffered.len() as f64);
      let (values, waiting): (Vec<_>, Vec<_>) = buffered.into_iter().unzip();
      let result =
        exec_write(&metrics, &*backend, values, alloc.as_ref()).map_err(|err| err.kind());
      for done in waiting {
        done.fulfill(result.map_err(Error::from));
      }
    }
  }
}

pub struct SyncScheduler(BatchExecutor<IOTask<(), ()>>);
impl SyncScheduler {
  const MAX_FLUSH_COUNT: usize = 512;
  pub fn new(
    pool: Arc<ThreadPool>,
    state: Arc<HandleState>,
    backend: Arc<dyn IOBackend>,
    metrics: Arc<MetricsRegistry>,
  ) -> Self {
    let executor = BatchExecutor::new(
      pool,
      Self::handle(state, backend, metrics),
      Self::MAX_FLUSH_COUNT,
    );
    Self(executor)
  }

  pub fn schedule(&self) -> Oneshot<Result<()>> {
    let (o, f) = oneshot();
    self.0.dispatch(((), f));
    o
  }

  const fn handle(
    state: Arc<HandleState>,
    backend: Arc<dyn IOBackend>,
    metrics: Arc<MetricsRegistry>,
  ) -> impl FnMut(Vec<IOTask<(), ()>>) {
    move |buffered| {
      let Some(_token) = state.pin.try_shared() else {
        // If truncate/remove owns the handle exclusively, this request no longer has a
        // meaningful file to operate on. Mark the handle closed and complete queued
        // waiters as successful no-ops.
        state.closed.fetch_or(true, Ordering::Release);
        return buffered
          .into_iter()
          .for_each(|(_, done)| done.fulfill(Ok(())));
      };

      metrics.disk_sync_batch.record(buffered.len() as f64);
      let result = backend.fdatasync().map_err(|err| err.kind());
      for (_, done) in buffered {
        done.fulfill(result.map_err(Error::from));
      }
    }
  }
}
