use std::{
  fs::{DirEntry, OpenOptions},
  io::{Error as IOError, ErrorKind, IoSlice, Result as IOResult},
  mem::forget,
  path::{Path, PathBuf},
  sync::{Arc, Mutex},
  thread::sleep,
  time::Duration,
};

use crossbeam::utils::Backoff;

use super::{
  AllocState, AppendIOHandle, DirHandle, DiskBackend, HandleState, IOBackend,
  ScanIOHandle, SyncHandle, WriteHandle,
};
use crate::{
  background::{Close, Oneshot, ThreadBuilder, ThreadPool},
  error, measure,
  metrics::MetricsRegistry,
  utils::{SBox, ShortenedMutex, ToArc},
  Error, Result,
};

const RETRY_INTERVAL: Duration = Duration::from_secs(5);
const MAX_RETRY: u8 = 10;

pub enum PendingIO<T = ()> {
  Fulfilled(IOResult<T>),
  Pending(Oneshot<std::result::Result<T, ErrorKind>>),
}
impl<T> PendingIO<T> {
  pub fn wait(self) -> IOResult<T> {
    match self {
      Self::Fulfilled(v) => v,
      Self::Pending(o) => o.wait().unwrap().map_err(IOError::from),
    }
  }

  pub fn wait_flatten(self) -> Result<T> {
    self.wait().map_err(Error::IO)
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
  thread: Arc<ThreadPool>,
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
      .to_arc();

    // The base directory lock prevents multiple engine processes from using the
    // same database directory. Retry is a courtesy delay, not a recovery protocol:
    // if another process keeps the lock, opening the pool fails.
    let base_dir = DirHandle::ensure(
      base_path,
      Box::new(backend),
      thread.clone(),
      metrics.clone(),
    )
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
    let state = Arc::new(HandleState::new());

    let write_handle = WriteHandle::new(
      self.thread.clone(),
      state.clone(),
      file.clone(),
      self.metrics.clone(),
      Some(AllocState::new(allocated)),
    );
    let sync_handle = SyncHandle::new(
      self.thread.clone(),
      state.clone(),
      file.clone(),
      self.metrics.clone(),
    );

    Ok(IOHandle {
      backend: file,
      write_handle,
      sync_handle,
      state,
      metrics: self.metrics.clone(),
      base_dir: self.base_dir.clone(),
      filename: Mutex::new(filename),
    })
  }

  pub fn open_static_sized(&self, filename: PathBuf, size: u64) -> Result<IOHandle> {
    let file = self.open_direct(&filename)?;
    file.fallocate(0, size).map_err(Error::IO)?;
    let state = Arc::new(HandleState::new());

    let write_handle = WriteHandle::new(
      self.thread.clone(),
      state.clone(),
      file.clone(),
      self.metrics.clone(),
      None,
    );
    let sync_handle = SyncHandle::new(
      self.thread.clone(),
      state.clone(),
      file.clone(),
      self.metrics.clone(),
    );

    Ok(IOHandle {
      backend: file,
      write_handle,
      sync_handle,
      state,
      metrics: self.metrics.clone(),
      base_dir: self.base_dir.clone(),
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
  write_handle: WriteHandle,
  sync_handle: SyncHandle,
  state: Arc<HandleState>,
  metrics: Arc<MetricsRegistry>,
  base_dir: SBox<DirHandle>,
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
      Ok(_) => Err(IOError::from(ErrorKind::UnexpectedEof)),
      Err(err) => Err(err),
    }
  }

  pub fn write(&self, buf: &'static [u8], offset: u64) -> PendingIO {
    if self.state.is_closed() {
      return PendingIO::Fulfilled(Ok(()));
    }
    PendingIO::Pending(self.write_handle.publish((offset, IoSlice::new(buf))))
  }

  pub fn fdatasync(&self) -> PendingIO {
    if self.state.is_closed() {
      return PendingIO::Fulfilled(Ok(()));
    }
    PendingIO::Pending(self.sync_handle.publish())
  }

  pub fn fsync(&self) -> IOResult<()> {
    let Some(_token) = self.state.try_shared() else {
      return Ok(());
    };
    self.backend.fsync()
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

  pub fn filename(&self) -> PathBuf {
    self.filename.l().clone()
  }
}
