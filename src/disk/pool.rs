use std::{
  fs::{create_dir_all, remove_file, rename, File, OpenOptions},
  io::{Error as IoError, ErrorKind, IoSlice},
  mem::forget,
  path::{Path, PathBuf},
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
  },
};

use crossbeam::{queue::SegQueue, utils::Backoff};

use super::{max_iov, DirectIO, Pread, Pwrite, Pwritev};
use crate::{
  disk::Fallocate,
  metrics::MetricsRegistry,
  thread::{oneshot, BackgroundThread, OneshotFulfill, TaskHandle, WorkBuilder},
  utils::{ExclusivePin, ShortenedMutex, ToArc},
  Error, Result,
};

type ThreadArg = (Arc<File>, IOTask, Arc<HandleState>);
type IOThread = dyn BackgroundThread<ThreadArg, ()>;
type WriteTask = (u64, IoSlice<'static>);

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
}

enum IOTask {
  Write(Arc<Task<WriteTask>>),
  Sync(Arc<Task<()>>),
}
struct Task<T> {
  queue: SegQueue<(T, OneshotFulfill<Result>)>,
  occupied: AtomicBool,
}
impl<T> Task<T> {
  const fn new() -> Self {
    Self {
      queue: SegQueue::new(),
      occupied: AtomicBool::new(false),
    }
  }
}

pub struct IOPool {
  thread: Arc<IOThread>,
  metrics: Arc<MetricsRegistry>,
}
impl IOPool {
  pub fn new(thread_count: usize, metrics: Arc<MetricsRegistry>) -> Self {
    let thread = WorkBuilder::new()
      .name("io pool")
      .multi(thread_count)
      .shared(handle_thread(metrics.clone()))
      .to_arc();
    Self { thread, metrics }
  }

  pub fn create_dir(&self, path: &Path) -> Result<DirHandle> {
    create_dir_all(path).map_err(Error::IO)?;
    Ok(DirHandle {
      file: File::open(path).map_err(Error::IO)?.to_arc(),
      sync_handle: Task::new().to_arc(),
      thread: self.thread.clone(),
      state: HandleState::new().to_arc(),
    })
  }

  pub fn create_handle(&self, path: &Path) -> Result<IOHandle> {
    // Direct IO bypasses the OS page cache for predictable latency.
    // To compensate for the lack of OS write buffering, writes are
    // accumulated and sorted in the eager_buffering layer, then
    // flushed as a single pwritev call per contiguous block.
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .direct_io(path)
      .map_err(Error::IO)?
      .to_arc();
    Ok(IOHandle {
      file,
      write_handle: Task::new().to_arc(),
      sync_handle: Task::new().to_arc(),
      state: HandleState::new().to_arc(),
      thread: self.thread.clone(),
      metrics: self.metrics.clone(),
      path: Mutex::new(PathBuf::from(path)),
    })
  }

  pub fn close(&self) {
    self.thread.close();
  }
}
const MAX_FLUSH_COUNT: usize = 512;
const fn handle_thread(metrics: Arc<MetricsRegistry>) -> impl Fn(ThreadArg) {
  let count = max_iov();
  move |(file, task, state)| {
    metrics.active_io_threads.inc();

    match task {
      IOTask::Write(handle) => {
        let mut buffered = Vec::with_capacity(count);

        loop {
          for task in (0..count).map_while(|_| handle.queue.pop()) {
            buffered.push(task);
          }

          flush_write(&metrics, &file, &state, &mut buffered);
          handle.occupied.fetch_and(false, Ordering::Release);
          if handle.queue.is_empty() {
            break;
          }
          if handle.occupied.fetch_or(true, Ordering::AcqRel) {
            break;
          }
        }
      }
      IOTask::Sync(handle) => {
        let mut buffered = Vec::with_capacity(MAX_FLUSH_COUNT);

        loop {
          for (_, fulfill) in (0..MAX_FLUSH_COUNT).map_while(|_| handle.queue.pop()) {
            buffered.push(fulfill);
          }

          flush_fdatasync(&file, &state, &mut buffered);
          handle.occupied.fetch_and(false, Ordering::Release);
          if handle.queue.is_empty() {
            break;
          }
          if handle.occupied.fetch_or(true, Ordering::AcqRel) {
            break;
          }
        }
      }
    }

    metrics.active_io_threads.dec();
  }
}

fn flush_fdatasync(
  file: &File,
  state: &HandleState,
  waiting: &mut Vec<OneshotFulfill<Result>>,
) {
  if waiting.is_empty() {
    return;
  }

  let result = match state.pin.try_shared() {
    Some(_t) => file.sync_data().map_err(Error::IO),
    None => {
      state.closed.fetch_or(true, Ordering::Release);
      return waiting.drain(..).for_each(|done| done.fulfill(Ok(())));
    }
  };
  waiting
    .drain(..)
    .for_each(|done| done.fulfill(result.clone()))
}

fn flush_write(
  metrics: &MetricsRegistry,
  file: &File,
  state: &HandleState,
  buffered: &mut Vec<(WriteTask, OneshotFulfill<Result>)>,
) {
  if buffered.is_empty() {
    return;
  }

  let (values, waiting): (Vec<_>, Vec<_>) = buffered.drain(..).unzip();
  let result = match state.pin.try_shared() {
    Some(_t) => write_exec(metrics, file, values).map_err(Error::IO),
    None => {
      state.closed.fetch_or(true, Ordering::Release);
      return waiting.into_iter().for_each(|done| done.fulfill(Ok(())));
    }
  };
  waiting
    .into_iter()
    .for_each(|done| done.fulfill(result.clone()));
}

fn write_exec(
  metrics: &MetricsRegistry,
  file: &File,
  mut buffered: Vec<WriteTask>,
) -> std::io::Result<()> {
  if buffered.len() == 1 {
    let (p, buf) = &buffered[0];
    return metrics
      .disk_write
      .measure(|| file.pwrite_all(buf, *p))
      .map(|_| ());
  }

  // last caller wins on duplicate pointers
  buffered.sort_by_key(|(i, _)| *i);
  buffered.reverse();
  buffered.dedup_by_key(|(i, b)| (*i, b.len()));
  buffered.reverse();

  for chunk in buffered.chunk_by(|(a_o, a_b), (b_o, _)| a_o + a_b.len() as u64 == *b_o) {
    let (offset, mut bufs): (Vec<_>, Vec<_>) =
      chunk.into_iter().map(|(o, b)| (*o, *b)).unzip();
    let offset = offset[0];
    if bufs.len() == 1 {
      metrics
        .disk_write
        .measure(|| file.pwrite_all(&bufs[0], offset))?;
      continue;
    }

    metrics
      .disk_write
      .measure(|| file.pwritev_all(&mut bufs, offset))?;
  }
  Ok(())
}

pub struct IOHandle {
  file: Arc<File>,
  write_handle: Arc<Task<WriteTask>>,
  sync_handle: Arc<Task<()>>,
  thread: Arc<IOThread>,
  state: Arc<HandleState>,
  metrics: Arc<MetricsRegistry>,
  path: Mutex<PathBuf>,
}
impl IOHandle {
  pub fn read(&self, offset: u64, buf: &mut [u8]) -> Result {
    self
      .metrics
      .disk_read
      .measure(|| self.file.pread_exact(buf, offset))
      .map_err(Error::IO)
  }

  pub fn read_unchecked(&self, mut offset: u64, mut buf: &mut [u8]) -> Result {
    let full = buf.len();
    while !buf.is_empty() {
      match self.file.pread(buf, offset) {
        Ok(0) if buf.len() == full => break, // allow only empty, not partial.
        Ok(0) => return Err(Error::IO(IoError::from(ErrorKind::UnexpectedEof))),
        Ok(n) => {
          let tmp = buf;
          buf = &mut tmp[n..];
          offset += n as u64;
        }
        Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
        Err(e) => return Err(Error::IO(e)),
      }
    }
    Ok(())
  }

  pub fn write_async(&self, offset: u64, buf: &'static [u8]) -> TaskHandle<()> {
    let (o, f) = oneshot();
    let handle = TaskHandle::new(o);
    if self.state.closed.load(Ordering::Acquire) {
      f.fulfill(Ok(()));
      return handle;
    }

    self
      .write_handle
      .queue
      .push(((offset, IoSlice::new(buf)), f));
    if self.write_handle.occupied.fetch_or(true, Ordering::Release) {
      return handle;
    }

    self.thread.dispatch((
      self.file.clone(),
      IOTask::Write(self.write_handle.clone()),
      self.state.clone(),
    ));
    handle
  }

  pub fn fdatasync(&self) -> TaskHandle<()> {
    let (o, f) = oneshot();
    let handle = TaskHandle::new(o);
    if self.state.closed.load(Ordering::Acquire) {
      f.fulfill(Ok(()));
      return handle;
    }

    self.sync_handle.queue.push(((), f));
    if self.sync_handle.occupied.fetch_or(true, Ordering::Release) {
      return handle;
    }

    self.thread.dispatch((
      self.file.clone(),
      IOTask::Sync(self.sync_handle.clone()),
      self.state.clone(),
    ));
    handle
  }

  pub fn fsync(&self) -> Result {
    let _token = match self.state.pin.try_shared() {
      Some(token) => token,
      None => return Ok(()),
    };

    self.file.sync_all().map_err(Error::IO)
  }

  pub fn len(&self) -> Result<u64> {
    Ok(self.file.metadata().map_err(Error::IO)?.len())
  }

  pub fn truncate(&self) -> Result {
    let backoff = Backoff::new();
    loop {
      match self.state.pin.try_exclusive() {
        Some(t) => break forget(t),
        None => backoff.snooze(),
      }
    }

    remove_file(self.path.l().as_path()).map_err(Error::IO)
  }

  pub fn rename(&self, new_path: &Path) -> Result {
    let mut path = self.path.l();
    rename(path.as_path(), new_path).map_err(Error::IO)?;
    *path = PathBuf::from(new_path);
    Ok(())
  }

  pub fn fallocate(&self, offset: u64, len: u64) -> Result {
    self.file.fallocate(offset, len).map_err(Error::IO)
  }
}

pub struct DirHandle {
  file: Arc<File>,
  sync_handle: Arc<Task<()>>,
  thread: Arc<IOThread>,
  state: Arc<HandleState>,
}
impl DirHandle {
  pub fn fdatasync(&self) -> Result {
    let (o, f) = oneshot();
    let handle = TaskHandle::new(o);
    if self.state.closed.load(Ordering::Acquire) {
      f.fulfill(Ok(()));
      return handle.wait();
    }

    self.sync_handle.queue.push(((), f));
    if self.sync_handle.occupied.fetch_or(true, Ordering::Release) {
      return handle.wait();
    }

    self.thread.dispatch((
      self.file.clone(),
      IOTask::Sync(self.sync_handle.clone()),
      self.state.clone(),
    ));
    handle.wait()
  }
}
