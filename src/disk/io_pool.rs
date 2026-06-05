use std::{
  fs::{
    create_dir_all, exists, read_dir, remove_file, rename, DirEntry, File, OpenOptions,
  },
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
  utils::{ExclusivePin, SBox, ShortenedMutex, ToArc},
  Error, Result,
};

type ThreadArg = (SBox<File>, IOTask, SBox<HandleState>);
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
  Write(SBox<Task<WriteTask>>),
  Sync(SBox<Task<()>>),
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
  base_dir: SBox<DirHandle>,
}
impl IOPool {
  pub fn new(
    thread_count: usize,
    base_path: &Path,
    metrics: Arc<MetricsRegistry>,
  ) -> Result<Self> {
    let thread = WorkBuilder::new()
      .name("io pool")
      .multi(thread_count)
      .shared(handle_thread(metrics.clone()))
      .to_arc();
    let base_dir = SBox::new(DirHandle::ensure(base_path, thread.clone())?);
    Ok(Self {
      thread,
      metrics,
      base_dir,
    })
  }

  pub fn create_handle(&self, filename: PathBuf) -> Result<IOHandle> {
    // Direct IO bypasses the OS page cache for predictable latency.
    // To compensate for the lack of OS write buffering, writes are
    // accumulated and sorted in the eager_buffering layer, then
    // flushed as a single pwritev call per contiguous block.
    let path = self.base_dir.get_path().join(&filename);
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .direct_io(&path)
      .map_err(Error::IO)?;
    Ok(IOHandle {
      file: SBox::new(file),
      write_handle: SBox::new(Task::new()),
      sync_handle: SBox::new(Task::new()),
      state: SBox::new(HandleState::new()),
      thread: self.thread.clone(),
      metrics: self.metrics.clone(),
      base_dir: self.base_dir.clone(),
      filename: Mutex::new(PathBuf::from(filename)),
    })
  }

  pub fn sync_dir(&self) -> Result {
    self.base_dir.fdatasync()
  }
  pub fn read_dir(&self) -> Result<Vec<DirEntry>> {
    self.base_dir.read()
  }
  pub fn remove(&self, filename: &Path) -> Result {
    self.base_dir.remove(filename)
  }
  pub fn exists(&self, filename: &Path) -> Result<bool> {
    self.base_dir.exists(filename)
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
  file: SBox<File>,
  write_handle: SBox<Task<WriteTask>>,
  sync_handle: SBox<Task<()>>,
  thread: Arc<IOThread>,
  state: SBox<HandleState>,
  metrics: Arc<MetricsRegistry>,
  base_dir: SBox<DirHandle>,
  filename: Mutex<PathBuf>,
}
impl IOHandle {
  pub fn read(&self, buf: &mut [u8], offset: u64) -> Result {
    self
      .metrics
      .disk_read
      .measure(|| self.file.pread_exact(buf, offset))
      .map_err(Error::IO)
  }

  pub fn read_unchecked(&self, mut buf: &mut [u8], mut offset: u64) -> Result {
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

  pub fn write_async(&self, buf: &'static [u8], offset: u64) -> TaskHandle<()> {
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
    while self.state.pin.try_exclusive().map(forget).is_none() {
      backoff.snooze();
    }

    self.base_dir.remove(&*self.filename.l())
  }

  pub fn rename(&self, new_filename: PathBuf) -> Result {
    {
      let mut filename = self.filename.l();
      self.base_dir.rename(&*filename, &new_filename)?;
      *filename = new_filename;
    }
    Ok(())
  }

  pub fn fallocate(&self, offset: u64, len: u64) -> Result {
    self.file.fallocate(offset, len).map_err(Error::IO)
  }
  pub fn filename(&self) -> PathBuf {
    self.filename.l().clone()
  }
}

struct DirHandle {
  file: SBox<File>,
  sync_handle: SBox<Task<()>>,
  thread: Arc<IOThread>,
  state: SBox<HandleState>,
  path: PathBuf,
}
impl DirHandle {
  fn ensure(path: &Path, thread: Arc<IOThread>) -> Result<Self> {
    let (file, path) = create_dir_all(path)
      .and_then(|_| path.canonicalize())
      .and_then(|path| File::open(&path).map(|f| (f, path)))
      .map_err(Error::IO)?;

    Ok(Self {
      file: SBox::new(file),
      sync_handle: SBox::new(Task::new()),
      thread,
      state: SBox::new(HandleState::new()),
      path,
    })
  }
  fn fdatasync(&self) -> Result {
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

  fn get_path(&self) -> &Path {
    self.path.as_path()
  }
  fn read(&self) -> Result<Vec<DirEntry>> {
    let mut entries = Vec::new();

    for entry in read_dir(&self.path).map_err(Error::IO)? {
      let entry = entry.map_err(Error::IO)?;
      entries.push(entry);
    }

    Ok(entries)
  }
  fn remove(&self, filename: &Path) -> Result {
    remove_file(self.path.join(filename)).map_err(Error::IO)
  }
  fn exists(&self, filename: &Path) -> Result<bool> {
    exists(self.path.join(filename)).map_err(Error::IO)
  }
  fn rename(&self, from: &Path, to: &Path) -> Result {
    rename(self.path.join(from), self.path.join(to)).map_err(Error::IO)
  }
}
