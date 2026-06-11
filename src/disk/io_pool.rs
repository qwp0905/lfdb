use std::{
  fs::{DirEntry, OpenOptions},
  io::{
    BufReader, BufWriter, Error as IoError, ErrorKind, IoSlice, Read, Result as IOResult,
    Write,
  },
  mem::forget,
  path::{Path, PathBuf},
  sync::{Arc, Mutex},
};

use crossbeam::utils::Backoff;

use super::{
  create_io_thread, DiskBackend, HandleState, IOBackend, IOThread, TaskPublisher,
  WriteTask,
};
use crate::{
  background::{Oneshot, WorkBuilder},
  metrics::MetricsRegistry,
  utils::{SBox, ShortenedMutex, ToArc},
  Error, Result,
};

pub struct AsyncIO<T = ()>(Oneshot<IOResult<T>>);
impl<T> AsyncIO<T> {
  pub const fn new(inner: Oneshot<IOResult<T>>) -> Self {
    Self(inner)
  }

  pub fn wait(self) -> Result<T> {
    self.0.wait()?.map_err(Error::IO)
  }
}

pub struct IOPool {
  thread: Arc<IOThread>,
  metrics: Arc<MetricsRegistry>,
  base_dir: SBox<DirHandle>,
}
impl IOPool {
  pub fn with_backend<T: DiskBackend>(
    backend: T,
    thread_count: usize,
    base_path: &Path,
    metrics: Arc<MetricsRegistry>,
  ) -> Result<Self> {
    let thread = WorkBuilder::new()
      .name("io pool")
      .multi(thread_count)
      .shared(create_io_thread(metrics.clone()))
      .to_arc();
    let base_dir = SBox::new(
      DirHandle::ensure(base_path, Box::new(backend), thread.clone())
        .map_err(Error::IO)?,
    );
    Ok(Self {
      thread,
      metrics,
      base_dir,
    })
  }

  pub fn open_append_io(&self, filename: PathBuf) -> Result<AppendIOHandle> {
    let path = self.base_dir.get_path().join(&filename);
    let mut options = OpenOptions::new();
    let file = self
      .base_dir
      .open(options.write(true).append(true).create(true), &path)
      .map_err(Error::IO)?;
    Ok(AppendIOHandle {
      file: BufWriter::new(file),
      filename,
    })
  }
  pub fn open_scan_io(&self, filename: PathBuf) -> Result<ScanIOHandle> {
    let path = self.base_dir.get_path().join(&filename);
    let mut options = OpenOptions::new();
    let file = self
      .base_dir
      .open(options.read(true), &path)
      .map_err(Error::IO)?;
    let len = file.metadata().map_err(Error::IO)?.len();
    Ok(ScanIOHandle {
      file: BufReader::new(file),
      len,
      base_dir: self.base_dir.clone(),
      filename,
    })
  }
  pub fn open_direct_io(&self, filename: PathBuf) -> Result<IOHandle> {
    // Direct IO bypasses the OS page cache for predictable latency.
    // To compensate for the lack of OS write buffering, writes are
    // accumulated and sorted in the eager_buffering layer, then
    // flushed as a single pwritev call per contiguous block.
    let path = self.base_dir.get_path().join(&filename);
    let mut options = OpenOptions::new();
    let file = self
      .base_dir
      .open_direct_io(options.read(true).write(true).create(true), &path)
      .map_err(Error::IO)?;
    Ok(IOHandle {
      backend: file,
      write_handle: SBox::new(TaskPublisher::new()),
      sync_handle: SBox::new(TaskPublisher::new()),
      state: SBox::new(HandleState::new()),
      thread: self.thread.clone(),
      metrics: self.metrics.clone(),
      base_dir: self.base_dir.clone(),
      filename: Mutex::new(filename),
    })
  }

  pub fn sync_dir(&self) -> Result {
    self.base_dir.fdatasync().wait()?.map_err(Error::IO)
  }
  pub fn read_dir(&self) -> Result<Vec<DirEntry>> {
    self.base_dir.read().map_err(Error::IO)
  }
  pub fn remove(&self, filename: &Path) -> Result<()> {
    self.base_dir.remove(filename).map_err(Error::IO)
  }
  pub fn exists(&self, filename: &Path) -> Result<bool> {
    self.base_dir.exists(filename).map_err(Error::IO)
  }

  pub fn close(&self) {
    self.thread.close();
  }
}

pub struct IOHandle {
  backend: Arc<dyn IOBackend>,
  write_handle: SBox<TaskPublisher<WriteTask>>,
  sync_handle: SBox<TaskPublisher<()>>,
  thread: Arc<IOThread>,
  state: SBox<HandleState>,
  metrics: Arc<MetricsRegistry>,
  base_dir: SBox<DirHandle>,
  filename: Mutex<PathBuf>,
}
impl IOHandle {
  pub fn read(&self, buf: &mut [u8], offset: u64) -> IOResult<()> {
    // SAFETY: Since the removed table cannot access this path, a pin guarantee is not required.
    // If a path for read access to the removed table is established, pin guarantees are required.
    self
      .metrics
      .disk_read
      .measure(|| self.backend.pread_exact(buf, offset))
  }

  pub fn read_unchecked(&self, mut buf: &mut [u8], mut offset: u64) -> IOResult<()> {
    let full = buf.len();
    while !buf.is_empty() {
      match self.backend.pread(buf, offset) {
        Ok(0) if buf.len() == full => break, // allow only empty, not partial.
        Ok(0) => return Err(IoError::from(ErrorKind::UnexpectedEof)),
        Ok(n) => {
          let tmp = buf;
          buf = &mut tmp[n..];
          offset += n as u64;
        }
        Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
        Err(e) => return Err(e),
      }
    }
    Ok(())
  }

  pub fn write_async(&self, buf: &'static [u8], offset: u64) -> Oneshot<IOResult<()>> {
    self.write_handle.publish_write(
      &self.state,
      &*self.thread,
      &self.backend,
      (offset, IoSlice::new(buf)),
    )
  }

  pub fn fdatasync(&self) -> Oneshot<IOResult<()>> {
    self
      .sync_handle
      .publish_sync(&self.state, &*self.thread, &self.backend)
  }

  pub fn fsync(&self) -> IOResult<()> {
    let Some(_token) = self.state.try_shared() else {
      return Ok(());
    };
    self.backend.fsync()
  }

  pub fn len(&self) -> IOResult<u64> {
    Ok(self.backend.metadata()?.len())
  }

  pub fn truncate(&self) -> IOResult<()> {
    let backoff = Backoff::new();
    while self.state.try_exclusive().map(forget).is_none() {
      backoff.snooze();
    }

    self.base_dir.remove(&*self.filename.l())
  }

  pub fn rename(&self, new_filename: PathBuf) -> IOResult<()> {
    {
      let mut filename = self.filename.l();
      self.base_dir.rename(&*filename, &new_filename)?;
      *filename = new_filename;
    }
    Ok(())
  }

  pub fn fallocate(&self, offset: u64, len: u64) -> IOResult<()> {
    self.backend.fallocate(offset, len)
  }
  pub fn filename(&self) -> PathBuf {
    self.filename.l().clone()
  }
}

struct DirHandle {
  io_backend: Arc<dyn IOBackend>,
  disk_backend: Box<dyn DiskBackend>,
  sync_handle: SBox<TaskPublisher<()>>,
  thread: Arc<IOThread>,
  state: SBox<HandleState>,
  path: PathBuf,
}
impl DirHandle {
  fn ensure(
    path: &Path,
    disk_backend: Box<dyn DiskBackend>,
    thread: Arc<IOThread>,
  ) -> IOResult<Self> {
    let mut options = OpenOptions::new();
    let (file, path) = disk_backend
      .ensure_dir(path)
      .and_then(|_| path.canonicalize())
      .and_then(|path| {
        disk_backend
          .open(options.read(true), &path)
          .map(|f| (f, path))
      })?;
    Ok(Self {
      io_backend: Arc::from(file),
      disk_backend,
      sync_handle: SBox::new(TaskPublisher::new()),
      thread,
      state: SBox::new(HandleState::new()),
      path,
    })
  }
  fn fdatasync(&self) -> Oneshot<IOResult<()>> {
    self
      .sync_handle
      .publish_sync(&self.state, &*self.thread, &self.io_backend)
  }
  fn get_path(&self) -> &Path {
    self.path.as_path()
  }
  fn read(&self) -> IOResult<Vec<DirEntry>> {
    let mut entries = Vec::new();

    for entry in self.disk_backend.read_dir(&self.path)? {
      entries.push(entry?);
    }

    Ok(entries)
  }
  fn remove(&self, filename: &Path) -> IOResult<()> {
    self.disk_backend.remove_file(&self.path.join(filename))
  }
  fn exists(&self, filename: &Path) -> IOResult<bool> {
    self.disk_backend.exists(&self.path.join(filename))
  }
  fn rename(&self, from: &Path, to: &Path) -> IOResult<()> {
    self
      .disk_backend
      .rename(&self.path.join(from), &self.path.join(to))
  }
  fn open(&self, options: &mut OpenOptions, path: &Path) -> IOResult<Box<dyn IOBackend>> {
    self.disk_backend.open(options, path)
  }
  fn open_direct_io(
    &self,
    options: &mut OpenOptions,
    path: &Path,
  ) -> IOResult<Arc<dyn IOBackend>> {
    self
      .disk_backend
      .open_direct_io(options, path)
      .map(Arc::from)
  }
}

pub struct AppendIOHandle {
  file: BufWriter<Box<dyn IOBackend>>,
  filename: PathBuf,
}
impl AppendIOHandle {
  pub fn append(&mut self, buf: &[u8]) -> Result {
    self.file.write_all(buf).map_err(Error::IO)
  }
  pub fn flush(mut self) -> Result<PathBuf> {
    self.file.flush().map_err(Error::IO)?;
    self.file.get_ref().fsync().map_err(Error::IO)?;
    Ok(self.filename)
  }
}
pub struct ScanIOHandle {
  file: BufReader<Box<dyn IOBackend>>,
  len: u64,
  base_dir: SBox<DirHandle>,
  filename: PathBuf,
}
impl ScanIOHandle {
  pub fn read_to_vec(&mut self, bytes: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0; bytes];
    self.read(&mut buf)?;
    Ok(buf)
  }
  pub fn read(&mut self, buf: &mut [u8]) -> Result {
    self.file.read_exact(buf).map_err(Error::IO)
  }
  pub const fn len(&self) -> u64 {
    self.len
  }
  pub fn truncate(&self) -> Result {
    self.base_dir.remove(&self.filename).map_err(Error::IO)
  }
}
