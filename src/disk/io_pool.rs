use std::{
  fs::{DirEntry, OpenOptions},
  io::{Error as IOError, ErrorKind, Result as IOResult},
  mem::forget,
  path::{Path, PathBuf},
  sync::{Arc, Mutex},
  thread::sleep,
  time::Duration,
};

use crossbeam::utils::Backoff;

use super::{
  AllocState, AppendIOHandle, DirHandle, DiskBackend, HandleState, IOBackend, IOTask,
  PendingAsync, ScanIOHandle, SyncBatch, WriteBatch, NO_CALLBACK,
};
use crate::{
  background::{Callback, Oneshot},
  metrics::{measure, MetricsRegistry},
  utils::{error, ShortenedMutex},
  Error, Result,
};

const RETRY_INTERVAL: Duration = Duration::from_secs(5);
const MAX_RETRY: u8 = 10;

pub enum PendingIO<T: 'static = ()> {
  Fulfilled(IOResult<T>),
  Scheduled(Oneshot<IOResult<T>>),
  Direct(PendingAsync<T>),
}
impl<T> PendingIO<T> {
  pub fn wait(self) -> IOResult<T> {
    match self {
      Self::Fulfilled(v) => v,
      Self::Scheduled(o) => o.wait().unwrap(),
      Self::Direct(o) => o.wait(),
    }
  }

  pub fn wait_flatten(self) -> Result<T> {
    self.wait().map_err(Error::IO)
  }

  pub fn add_callback<F: FnOnce(&IOResult<T>) + Send + 'static>(self, f: F) {
    match self {
      PendingIO::Fulfilled(v) => f(&v),
      PendingIO::Scheduled(o) => o.must_call(Callback::new(f)),
      PendingIO::Direct(o) => o.must_call(Callback::new(f)),
    };
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
  metrics: Arc<MetricsRegistry>,
  base_dir: Arc<DirHandle>,
}
impl IOPool {
  pub fn with_backend<T: DiskBackend + 'static>(
    backend: T,
    base_path: &Path,
    metrics: Arc<MetricsRegistry>,
  ) -> Result<Self> {
    // The base directory lock prevents multiple engine processes from using the
    // same database directory. Retry is a courtesy delay, not a recovery protocol:
    // if another process keeps the lock, opening the pool fails.
    let base_dir = DirHandle::ensure(base_path, Box::new(backend), metrics.clone())
      .map_err(Error::IO)
      .map(Arc::new)?;
    for _ in 0..MAX_RETRY {
      if base_dir.try_lock().map_err(Error::IO)? {
        return Ok(Self { metrics, base_dir });
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
    Ok(AppendIOHandle::new(file))
  }
  pub fn open_scan_io(&self, filename: PathBuf) -> Result<ScanIOHandle> {
    let path = self.base_dir.get_path().join(&filename);
    let mut options = OpenOptions::new();
    let file = self
      .base_dir
      .open_direct_io(options.read(true), &path)
      .map_err(Error::IO)?;
    let len = file.metadata().map_err(Error::IO)?.len();
    Ok(ScanIOHandle::new(
      file,
      self.base_dir.clone(),
      filename,
      len,
    ))
  }

  fn open_direct(&self, filename: &PathBuf) -> Result<Arc<dyn IOBackend>> {
    // Direct IO bypasses the OS page cache for predictable latency.
    // To compensate for the lack of OS write buffering, writes are
    // accumulated and sorted in the eager_buffering layer, then
    // flushed as a single pwritev call per contiguous block.

    let path = self.base_dir.get_path().join(filename);
    let mut options = OpenOptions::new();
    self
      .base_dir
      .open_direct_io(options.read(true).write(true).create(true), &path)
      .map(Arc::<dyn IOBackend>::from)
      .map_err(Error::IO)
  }

  pub fn open_dynamic_sized(&self, filename: PathBuf) -> Result<IOHandle> {
    let file = self.open_direct(&filename)?;
    let allocated = file.metadata().map_err(Error::IO)?.len();
    Ok(self.create_handle(file, filename, Some(AllocState::new(allocated))))
  }

  fn create_handle(
    &self,
    backend: Arc<dyn IOBackend>,
    filename: PathBuf,
    alloc: Option<AllocState>,
  ) -> IOHandle {
    let state = Arc::new(HandleState::new());
    let write_handle = WriteBatch::default();
    let sync_handle = SyncBatch::default();
    let alloc = alloc.map(Arc::new);

    IOHandle {
      backend,
      write_scheduler: write_handle,
      sync_scheduler: sync_handle,
      state,
      alloc,
      metrics: self.metrics.clone(),
      base_dir: self.base_dir.clone(),
      filename: Mutex::new(filename),
    }
  }

  pub fn open_static_sized(&self, filename: PathBuf, size: u64) -> Result<IOHandle> {
    let file = self.open_direct(&filename)?;
    let (task, done) = IOTask::new_fallocate(0, size, NO_CALLBACK);
    file.submit(task);
    done.wait().map_err(Error::IO)?;
    Ok(self.create_handle(file, filename, None))
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
    self.base_dir.fdatasync().wait_flatten()
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
  write_scheduler: WriteBatch,
  sync_scheduler: SyncBatch,
  state: Arc<HandleState>,
  alloc: Option<Arc<AllocState>>,
  metrics: Arc<MetricsRegistry>,
  base_dir: Arc<DirHandle>,
  filename: Mutex<PathBuf>,
}
impl IOHandle {
  pub fn read(&self, buf: &mut [u8], offset: u64) -> IOResult<()> {
    // SAFETY: Since the removed table cannot access this path, a pin guarantee is not required.
    // If a path for read access to the removed table is established, pin guarantees are required.
    measure!(
      self.metrics.disk_read,
      self.backend.pread_exact(buf, offset)
    )
  }

  /**
   * Read a full buffer, but allow an immediate EOF.
   *
   * This differs from `read` only in how it treats a zero-byte read: `Ok(0)` is
   * accepted as an empty range. Any non-zero short read is still reported as
   * `UnexpectedEof`.
   */
  pub unsafe fn read_unchecked(&self, buf: &mut [u8], offset: u64) -> IOResult<()> {
    match self.backend.pread(buf, offset) {
      Ok(0) => Ok(()),
      Ok(n) if n == buf.len() => Ok(()),
      Ok(_) => Err(IOError::from(ErrorKind::UnexpectedEof)),
      Err(err) => Err(err),
    }
  }

  pub fn write_async(&self, buf: &'static [u8], offset: u64) -> PendingIO {
    if self.state.is_closed() {
      return PendingIO::Fulfilled(Ok(()));
    }
    let done = self.write_scheduler.publish_write(
      &self.state,
      &self.backend,
      &self.metrics,
      &self.alloc,
      buf,
      offset,
    );
    PendingIO::Scheduled(done)
  }

  pub fn fdatasync_async(&self) -> PendingIO {
    if self.state.is_closed() {
      return PendingIO::Fulfilled(Ok(()));
    }
    let done =
      self
        .sync_scheduler
        .publish_sync(&self.state, &self.backend, &self.metrics);
    PendingIO::Scheduled(done)
  }

  pub fn fsync(&self) -> PendingIO<usize> {
    let Some(_token) = self.state.try_shared() else {
      return PendingIO::Fulfilled(Ok(0));
    };
    let (task, done) = IOTask::new_fsync(NO_CALLBACK);
    self.backend.submit(task);
    PendingIO::Direct(done)
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
}
