use std::{
  fs::{DirEntry, OpenOptions},
  io::{Error as IoError, ErrorKind, IoSlice, Result as IOResult},
  mem::{forget, replace},
  path::{Path, PathBuf},
  sync::{Arc, Mutex},
  thread::sleep,
  time::Duration,
};

use crossbeam::utils::Backoff;

use super::{
  create_io_thread, AlignedArray, AllocState, DiskBackend, HandleState, IOBackend,
  IOThread, TaskPublisher, WriteTask, ALIGN,
};
use crate::{
  background::{Oneshot, ThreadBuilder},
  error, measure,
  metrics::MetricsRegistry,
  utils::{SBox, ShortenedMutex, ToArc},
  Error, Result,
};

const RETRY_INTERVAL: Duration = Duration::from_secs(5);
const MAX_RETRY: u8 = 10;

pub struct AsyncIO<T = ()>(Oneshot<IOResult<T>>);
impl<T> AsyncIO<T> {
  pub const fn new(inner: Oneshot<IOResult<T>>) -> Self {
    Self(inner)
  }

  pub fn wait(self) -> Result<T> {
    self.0.wait().unwrap().map_err(Error::IO)
  }
}

/**
 * Engine-local filesystem facade.
 *
 * `IOPool` owns the database base directory lock, keeps the shared IO worker
 * pool, exposes namespace operations, and creates the file handles used by the
 * storage layers. In practice this is the central entry point for disk access
 * inside the engine, not just a handle factory.
 */
pub struct IOPool {
  thread: Arc<IOThread>,
  metrics: Arc<MetricsRegistry>,
  base_dir: SBox<DirHandle>,
}
impl IOPool {
  pub fn with_backend<T: DiskBackend + 'static>(
    backend: T,
    thread_count: usize,
    base_path: &Path,
    metrics: Arc<MetricsRegistry>,
  ) -> Result<Self> {
    let thread = ThreadBuilder::new()
      .name("io pool")
      .multi(thread_count)
      .shared(create_io_thread(metrics.clone()))
      .to_arc();

    // The base directory lock prevents multiple engine processes from using the
    // same database directory. Retry is a courtesy delay, not a recovery protocol:
    // if another process keeps the lock, opening the pool fails.
    let base_dir = DirHandle::ensure(base_path, Box::new(backend), thread.clone())
      .map_err(Error::IO)
      .map(SBox::new)?;
    for _ in 0..MAX_RETRY {
      if base_dir.try_lock().map_err(Error::IO)? {
        return Ok(Self {
          thread,
          metrics,
          base_dir,
        });
      }

      error!(
        "dir {:?} are still in use. trying to retry in {} secs...",
        base_dir.get_path(),
        RETRY_INTERVAL.as_secs(),
      );
      sleep(RETRY_INTERVAL);
    }
    Err(Error::DirOpenFailed)
  }

  pub fn open_append_io(&self, filename: PathBuf) -> Result<AppendIOHandle> {
    let path = self.base_dir.get_path().join(&filename);
    let mut options = OpenOptions::new();
    let file = self
      .base_dir
      .open_direct_io(options.write(true).create(true), &path)
      .map_err(Error::IO)?;
    Ok(AppendIOHandle::new(file, filename))
  }
  pub fn open_scan_io(&self, filename: PathBuf) -> Result<ScanIOHandle> {
    let path = self.base_dir.get_path().join(&filename);
    let mut options = OpenOptions::new();
    let file = self
      .base_dir
      .open_direct_io(options.read(true), &path)
      .map_err(Error::IO)?;
    Ok(ScanIOHandle::new(file, self.base_dir.clone(), filename))
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
      .map(Arc::<dyn IOBackend>::from)
      .map_err(Error::IO)?;
    let allocated = file.metadata().map_err(Error::IO)?.len();
    Ok(IOHandle {
      backend: file,
      write_handle: SBox::new(TaskPublisher::new()),
      sync_handle: SBox::new(TaskPublisher::new()),
      state: SBox::new(HandleState::new()),
      thread: self.thread.clone(),
      metrics: self.metrics.clone(),
      base_dir: self.base_dir.clone(),
      allocated: SBox::new(AllocState::new(allocated)),
      filename: Mutex::new(filename),
    })
  }

  /**
   * Durably sync base-directory namespace changes.
   *
   * Call this after operations that must make directory entries durable, such as
   * creating, removing, or renaming files. The method is exposed as a low-level
   * primitive so the caller decides which namespace changes require a durability
   * boundary.
   */
  pub fn sync_dir(&self) -> Result {
    self.base_dir.fdatasync().wait().unwrap().map_err(Error::IO)
  }
  pub fn read_dir(&self) -> Result<Vec<DirEntry>> {
    self.base_dir.read().map_err(Error::IO)
  }
  pub fn truncate(&self, filename: &Path) -> Result<()> {
    self.base_dir.remove(filename).map_err(Error::IO)
  }
  pub fn exists(&self, filename: &Path) -> Result<bool> {
    self.base_dir.exists(filename).map_err(Error::IO)
  }

  pub fn close(&self) {
    self.thread.close();
  }
}
impl Drop for IOPool {
  fn drop(&mut self) {
    let _ = self.base_dir.unlock();
  }
}

/**
 * Main handle for one opened file backend.
 *
 * `IOHandle` owns an `IOBackend` and exposes the engine's general file access
 * operations for it: positioned reads, batched asynchronous writes, data sync,
 * full sync, preallocation, rename, truncate, and filename tracking. It is the
 * broadest file-handle abstraction in the disk layer.
 */
pub struct IOHandle {
  backend: Arc<dyn IOBackend>,
  write_handle: SBox<TaskPublisher<WriteTask>>,
  sync_handle: SBox<TaskPublisher<()>>,
  thread: Arc<IOThread>,
  state: SBox<HandleState>,
  metrics: Arc<MetricsRegistry>,
  base_dir: SBox<DirHandle>,
  allocated: SBox<AllocState>,
  filename: Mutex<PathBuf>,
}
impl IOHandle {
  pub fn read(&self, buf: &mut [u8], offset: u64) -> IOResult<()> {
    // SAFETY: Since the removed table cannot access this path, a pin guarantee is not required.
    // If a path for read access to the removed table is established, pin guarantees are required.
    measure!(
      self.metrics.disk_read,
      self.backend.pread_or_fail(buf, offset)
    )
  }

  /**
   * Read a full buffer, but allow an immediate EOF.
   *
   * This differs from `read` only in how it treats a zero-byte read: `Ok(0)` is
   * accepted as an empty range. Any non-zero short read is still reported as
   * `UnexpectedEof`.
   */
  pub fn read_unchecked(&self, buf: &mut [u8], offset: u64) -> IOResult<()> {
    match self.backend.pread(buf, offset) {
      Ok(0) => Ok(()),
      Ok(n) if n == buf.len() => Ok(()),
      Ok(_) => Err(IoError::from(ErrorKind::UnexpectedEof)),
      Err(err) => Err(err),
    }
  }

  pub fn alloc_and_write(
    &self,
    buf: &'static [u8],
    offset: u64,
  ) -> Oneshot<IOResult<()>> {
    self.write_handle.publish_alloc_and_write(
      &self.state,
      &*self.thread,
      &self.backend,
      &self.allocated,
      (offset, IoSlice::new(buf)),
    )
  }
  pub fn write_only(&self, buf: &'static [u8], offset: u64) -> Oneshot<IOResult<()>> {
    self.write_handle.publish_write_only(
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

  /**
   * Remove the file represented by this handle.
   *
   * The method waits until in-flight asynchronous file operations are no longer
   * using the handle, then removes the file from the base directory.
   */
  pub fn truncate(&self) -> IOResult<()> {
    let backoff = Backoff::new();
    while self.state.try_exclusive().map(forget).is_none() {
      backoff.snooze();
    }

    self.base_dir.remove(&self.filename.l())
  }

  pub fn rename(&self, new_filename: PathBuf) -> IOResult<()> {
    {
      let mut filename = self.filename.l();
      self.base_dir.rename(&filename, &new_filename)?;
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

/**
 * Base-directory-bound disk backend.
 *
 * `DirHandle` wraps the `DiskBackend` for namespace operations under one
 * canonical base path, and also keeps an opened directory handle so the pool can
 * lock and sync the directory itself.
 */
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
    disk_backend.ensure_dir(path)?;
    let path = path.canonicalize()?;
    let file = disk_backend.open(options.read(true), &path)?;
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
  fn open_direct_io(
    &self,
    options: &mut OpenOptions,
    path: &Path,
  ) -> IOResult<Box<dyn IOBackend>> {
    self.disk_backend.open_direct_io(options, path)
  }
  fn try_lock(&self) -> IOResult<bool> {
    self.io_backend.try_flock()
  }
  fn unlock(&self) -> IOResult<()> {
    self.io_backend.unlock()
  }
}

/**
 * Buffered writer for direct-I/O append-style output.
 *
 * Callers append arbitrary byte slices, and the handle packs them into an
 * internally aligned buffer before issuing positioned writes. The mutable API
 * makes this a single-writer stream handle.
 */
pub struct AppendIOHandle {
  file: Box<dyn IOBackend>,
  buffer: AlignedArray,
  buffer_offset: usize,
  file_offset: u64,
  filename: PathBuf,
}
impl AppendIOHandle {
  pub const fn new(file: Box<dyn IOBackend>, filename: PathBuf) -> Self {
    Self {
      file,
      filename,
      buffer: AlignedArray::new(),
      buffer_offset: 0,
      file_offset: 0,
    }
  }
  pub fn append(&mut self, mut buf: &[u8]) -> Result {
    loop {
      let end = self.buffer_offset + buf.len();
      if end <= ALIGN {
        self.buffer[replace(&mut self.buffer_offset, end)..end].copy_from_slice(buf);
        return Ok(());
      }

      let available = ALIGN - self.buffer_offset;
      self.buffer[self.buffer_offset..].copy_from_slice(&buf[..available]);
      self.flush_buf()?;
      buf = &buf[available..];
    }
  }
  fn flush_buf(&mut self) -> Result {
    self
      .file
      .pwrite_or_fail(&*self.buffer, self.file_offset)
      .map_err(Error::IO)?;
    self.file_offset += ALIGN as u64;
    self.buffer_offset = 0;
    Ok(())
  }
  /**
   * Flush the buffered stream, sync the file, and finish the writer.
   *
   * The handle is consumed because this is the finalization step for the append
   * stream.
   */
  pub fn flush(mut self) -> Result<PathBuf> {
    self.flush_buf()?;
    self.file.fsync().map_err(Error::IO)?;
    Ok(self.filename)
  }
}

/**
 * Buffered sequential reader for direct-I/O scan-style input.
 *
 * The handle reads through an internally aligned buffer and copies out exactly
 * the number of bytes requested by the caller. Like `AppendIOHandle`, it hides
 * the memory and disk alignment details from higher layers.
 */
pub struct ScanIOHandle {
  file: Box<dyn IOBackend>,
  buffer: AlignedArray,
  buffer_offset: usize,
  file_offset: u64,
  base_dir: SBox<DirHandle>,
  filename: PathBuf,
}
impl ScanIOHandle {
  const fn new(
    file: Box<dyn IOBackend>,
    base_dir: SBox<DirHandle>,
    filename: PathBuf,
  ) -> Self {
    Self {
      file,
      buffer: AlignedArray::new(),
      buffer_offset: ALIGN,
      file_offset: 0,
      base_dir,
      filename,
    }
  }
  fn fill_buf(&mut self) -> Result {
    self
      .file
      .pread_or_fail(&mut *self.buffer, self.file_offset)
      .map_err(Error::IO)?;
    self.file_offset += ALIGN as u64;
    self.buffer_offset = 0;
    Ok(())
  }
  pub fn read_to_vec(&mut self, bytes: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0; bytes];
    self.read(&mut buf)?;
    Ok(buf)
  }
  pub fn read(&mut self, mut buf: &mut [u8]) -> Result {
    loop {
      let end = self.buffer_offset + buf.len();
      if end <= ALIGN {
        buf.copy_from_slice(&self.buffer[replace(&mut self.buffer_offset, end)..end]);
        return Ok(());
      }

      let available = ALIGN - self.buffer_offset;
      buf[..available].copy_from_slice(&self.buffer[self.buffer_offset..]);
      self.fill_buf()?;
      buf = &mut buf[available..];
    }
  }
  pub fn len(&self) -> Result<u64> {
    Ok(self.file.metadata().map_err(Error::IO)?.len())
  }
  pub fn truncate(&self) -> Result {
    self.base_dir.remove(&self.filename).map_err(Error::IO)
  }
}
