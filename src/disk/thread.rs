use std::{
  cell::Cell,
  io::{ErrorKind, IoSlice, Result},
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
};

use super::{max_iov, IOBackend};
use crate::{
  background::{BatchExecutor, Oneshot, ThreadPool},
  measure,
  metrics::MetricsRegistry,
  utils::{ExclusivePin, ExclusiveToken, SharedToken},
};

pub type WriteTask = (u64, IoSlice<'static>);

/**
 * Tracks the file size already covered by preallocation.
 *
 * `AllocState` uses `Cell` because it is only accessed by the single worker that
 * owns the corresponding `AllocAndWrite` flush pass. The value is stored in an
 * `SBox` and can move across threads, but `TaskPublisher::occupied` serializes
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
      measure!(metrics.disk_write, backend.pwrite_or_fail(&bufs[0], offset))?;
      continue;
    }

    measure!(metrics.disk_write, backend.pwritev_or_fail(&bufs, offset))?;
  }
  Ok(())
}

pub struct WriteHandle(BatchExecutor<WriteTask, std::result::Result<(), ErrorKind>>);
impl WriteHandle {
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

  pub fn publish(&self, task: WriteTask) -> Oneshot<std::result::Result<(), ErrorKind>> {
    self.0.execute(task)
  }

  const fn handle(
    state: Arc<HandleState>,
    backend: Arc<dyn IOBackend>,
    metrics: Arc<MetricsRegistry>,
    alloc: Option<AllocState>,
  ) -> impl FnMut(Vec<WriteTask>) -> std::result::Result<(), ErrorKind> {
    move |buffered| {
      let Some(_token) = state.pin.try_shared() else {
        // If truncate/remove owns the handle exclusively, this request no longer has a
        // meaningful file to operate on. Mark the handle closed and complete queued
        // waiters as successful no-ops.
        state.closed.fetch_or(true, Ordering::Release);
        return Ok(());
      };

      exec_write(&metrics, &*backend, buffered, alloc.as_ref()).map_err(|err| err.kind())
    }
  }
}

pub struct SyncHandle(BatchExecutor<(), std::result::Result<(), ErrorKind>>);
impl SyncHandle {
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

  pub fn publish(&self) -> Oneshot<std::result::Result<(), ErrorKind>> {
    self.0.execute(())
  }

  const fn handle(
    state: Arc<HandleState>,
    backend: Arc<dyn IOBackend>,
    metrics: Arc<MetricsRegistry>,
  ) -> impl FnMut(Vec<()>) -> std::result::Result<(), ErrorKind> {
    move |buffered| {
      let Some(_token) = state.pin.try_shared() else {
        // If truncate/remove owns the handle exclusively, this request no longer has a
        // meaningful file to operate on. Mark the handle closed and complete queued
        // waiters as successful no-ops.
        state.closed.fetch_or(true, Ordering::Release);
        return Ok(());
      };

      metrics.disk_sync_batch.record(buffered.len() as f64);
      backend.fdatasync().map_err(|err| err.kind())
    }
  }
}
