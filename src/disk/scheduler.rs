use std::{
  cell::Cell,
  io::{Error, ErrorKind, IoSlice, Result},
  mem::replace,
  sync::{
    atomic::{fence, AtomicBool, AtomicU16, Ordering},
    Arc,
  },
  time::Instant,
};

use crossbeam::queue::SegQueue;

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

type WriteAcc = (IOTask, Oneshot<Result<usize>>, Callback<Result<usize>>);

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

impl BatchQueue<WriteTask, Result<()>> {
  fn recursive_write(self: &Arc<Self>, arg: WriteBatchArg) {
    if self.try_release() {
      return;
    }
    self.drain_write(arg);
  }
  fn accumulate_write(
    self: &Arc<Self>,
    arg: &WriteBatchArg,
    buffered: Vec<(WriteTask, OneshotFulfill<Result<()>>)>,
  ) -> Vec<WriteAcc> {
    let mut count = None;
    let mut accumulated = Vec::new();
    let mut waiting = Vec::new();
    let mut values = Vec::new();
    let mut start = 0;
    let mut prev = None;
    for ((offset, buf), done) in buffered {
      let Some((po, len)) = prev.replace((offset, buf.len() as u64)) else {
        start = offset;
        waiting.push(done);
        values.push(buf);
        continue;
      };

      if po == offset && len == buf.len() as u64 {
        waiting.push(done);
        *values.last_mut().unwrap() = buf;
        continue;
      }
      if po + len == offset {
        waiting.push(done);
        values.push(buf);
        continue;
      }

      prev = Some((offset, buf.len() as u64));
      let current = replace(&mut start, offset);
      let bufs = replace(&mut values, vec![buf]);
      let waiting = replace(&mut waiting, vec![done]);
      let static_ref = unsafe { create_static_ref::<[IoSlice<'static>]>(&bufs) };
      let (task, done) = if bufs.len() == 1 {
        IOTask::new_pwrite(&static_ref[0], current)
      } else {
        IOTask::new_pwritev(static_ref, current)
      };

      let bytes = bufs.iter().map(|v| v.len()).sum::<usize>();
      let count = count
        .get_or_insert_with(|| Arc::new(AtomicU16::new(0)))
        .clone();
      let queue = self.clone();
      let arg = arg.clone();
      let start = arg.metrics.disk_write.start();
      let callback = Callback::new(move |r: &Result<usize>| {
        let result = match r {
          Ok(c) if *c < bytes => {
            return queue.retry_write(arg, 1, current, bufs, waiting, Some(count), start);
          }
          Ok(_) => Ok(()),
          Err(err) => Err(err.kind()),
        };
        arg.metrics.disk_write.record(start);
        for done in waiting {
          done.fulfill(result.map_err(Error::from));
        }
        if count.fetch_sub(1, Ordering::Relaxed) > 1 {
          return;
        }
        queue.recursive_write(arg)
      });
      accumulated.push((task, done, callback));
    }

    let static_ref = unsafe { create_static_ref::<[IoSlice<'static>]>(&values) };
    let (task, done) = if values.len() == 1 {
      IOTask::new_pwrite(&static_ref[0], start)
    } else {
      IOTask::new_pwritev(static_ref, start)
    };

    let bytes = values.iter().map(|v| v.len()).sum::<usize>();
    let queue = self.clone();
    let arg = arg.clone();
    let metrics_start = arg.metrics.disk_write.start();

    let Some(count) = count else {
      let bytes = values.iter().map(|v| v.len()).sum::<usize>();
      let callback = Callback::new(move |r: &Result<usize>| {
        let result = match r {
          Ok(c) if *c < bytes => {
            return queue.retry_write(
              arg,
              1,
              start,
              values,
              waiting,
              None,
              metrics_start,
            );
          }
          Ok(_) => Ok(()),
          Err(err) => Err(err.kind()),
        };
        arg.metrics.disk_write.record(metrics_start);
        for done in waiting {
          done.fulfill(result.map_err(Error::from));
        }
        queue.recursive_write(arg)
      });
      accumulated.push((task, done, callback));
      return accumulated;
    };

    count.store(accumulated.len() as u16 + 1, Ordering::Relaxed);
    let callback = Callback::new(move |r: &Result<usize>| {
      let result = match r {
        Ok(c) if *c < bytes => {
          return queue.retry_write(
            arg,
            1,
            start,
            values,
            waiting,
            Some(count),
            metrics_start,
          );
        }
        Ok(_) => Ok(()),
        Err(err) => Err(err.kind()),
      };
      arg.metrics.disk_write.record(metrics_start);
      for done in waiting {
        done.fulfill(result.map_err(Error::from));
      }
      if count.fetch_sub(1, Ordering::Relaxed) > 1 {
        return;
      }
      queue.recursive_write(arg)
    });
    accumulated.push((task, done, callback));
    accumulated
  }
  fn finish_write(
    self: &Arc<Self>,
    arg: &WriteBatchArg,
    buffered: Vec<(WriteTask, OneshotFulfill<Result<()>>)>,
  ) {
    debug_assert!(!buffered.is_empty());
    let accumulated = self.accumulate_write(arg, buffered);
    let len = accumulated.len();
    debug_assert!(accumulated.len() < u16::MAX as usize);
    let mut callbacks = Vec::with_capacity(len);
    let mut tasks = Vec::with_capacity(len);
    for (task, done, callback) in accumulated {
      tasks.push(task);
      callbacks.push((done, callback));
    }

    if let Err(err) = arg.backend.batch_submit(tasks) {
      let kind = err.kind();
      for (_, callback) in callbacks {
        callback.call(&Err(Error::from(kind)));
      }
      return;
    }

    for (done, callback) in callbacks {
      if let Err(err) = done.add_callback(callback) {
        err.call(&done.wait().unwrap())
      }
    }
  }
  fn retry_write(
    self: &Arc<Self>,
    arg: WriteBatchArg,
    trial: u8,
    offset: u64,
    bufs: Vec<IoSlice<'static>>,
    waiting: Vec<OneshotFulfill<Result<()>>>,
    count: Option<Arc<AtomicU16>>,
    start: Option<Instant>,
  ) {
    if trial >= IO_RETRY {
      arg.metrics.disk_write.record(start);
      let kind = ErrorKind::WriteZero;
      waiting
        .into_iter()
        .for_each(|done| done.fulfill(Err(Error::from(kind))));
      if count.is_some_and(|c| c.fetch_sub(1, Ordering::Relaxed) > 1) {
        return;
      }
      return self.recursive_write(arg);
    }

    let static_ref = unsafe { create_static_ref::<[IoSlice<'static>]>(&bufs) };
    let (task, done) = if bufs.len() == 1 {
      IOTask::new_pwrite(&static_ref[0], offset)
    } else {
      IOTask::new_pwritev(static_ref, offset)
    };
    if let Err(err) = arg.backend.submit(task) {
      let kind = err.kind();
      waiting
        .into_iter()
        .for_each(|done| done.fulfill(Err(Error::from(kind))));
      if count.is_some_and(|c| c.fetch_sub(1, Ordering::Relaxed) > 1) {
        return;
      }
      return self.recursive_write(arg);
    }
    let bytes = bufs.iter().map(|v| v.len()).sum::<usize>();
    let count = count.clone();
    let queue = self.clone();
    let arg = arg.clone();
    let callback = Callback::new(move |r: &Result<usize>| {
      let result = match r {
        Ok(c) if *c < bytes => {
          return queue.retry_write(arg, trial + 1, offset, bufs, waiting, count, start);
        }
        Ok(_) => Ok(()),
        Err(err) => Err(err.kind()),
      };
      arg.metrics.disk_write.record(start);
      for done in waiting {
        done.fulfill(result.map_err(Error::from));
      }
      if count.is_some_and(|c| c.fetch_sub(1, Ordering::Relaxed) > 1) {
        return;
      }
      queue.recursive_write(arg)
    });
    if let Err(err) = done.add_callback(callback) {
      err.call(&done.wait().unwrap());
    }
  }
  fn drain_write(self: &Arc<Self>, arg: WriteBatchArg) {
    let count = max_iov();
    let mut buffered = Vec::with_capacity(count);
    for task in (0..count).map_while(|_| self.queue.pop()) {
      buffered.push(task);
    }

    if buffered.is_empty() {
      return self.recursive_write(arg);
    }

    let Some(token) = arg.state.pin.try_shared() else {
      arg.state.closed.fetch_or(true, Ordering::Relaxed);
      buffered
        .into_iter()
        .for_each(|(_, done)| done.fulfill(Ok(())));
      return self.recursive_write(arg);
    };
    arg.metrics.disk_write_batch.record(buffered.len() as f64);

    if buffered.len() > 1 {
      buffered.sort_by_key(|((i, _), _)| *i);
    }

    let Some(alloc) = arg.alloc.as_deref() else {
      return self.finish_write(&arg, buffered);
    };

    // Space allocation is owned by this batching layer. Since all writes for this
    // handle are flushed here, the worker can preallocate once up to the highest
    // required offset before issuing the actual writes.
    let required = buffered
      .last()
      .map(|((o, b), _)| *o + b.len() as u64)
      .unwrap();
    let (done, allocated) = match alloc_if_needed(required, alloc, &*arg.backend) {
      Ok(Some(v)) => v,
      Ok(None) => return self.finish_write(&arg, buffered),
      Err(err) => {
        drop(token);
        buffered
          .into_iter()
          .for_each(|(_, done)| done.fulfill(Err(Error::from(err.kind()))));
        return self.recursive_write(arg);
      }
    };

    drop(token);
    let queue = self.clone();
    let callback = Callback::new(move |r: &Result<usize>| {
      if let Err(err) = r {
        buffered
          .into_iter()
          .for_each(|(_, done)| done.fulfill(Err(Error::from(err.kind()))));
        return queue.recursive_write(arg);
      };

      let Some(_token) = arg.state.pin.try_shared() else {
        arg.state.closed.fetch_or(true, Ordering::Relaxed);
        buffered
          .into_iter()
          .for_each(|(_, done)| done.fulfill(Ok(())));
        return queue.recursive_write(arg);
      };
      arg.alloc.as_deref().unwrap().set(allocated);
      queue.finish_write(&arg, buffered);
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
