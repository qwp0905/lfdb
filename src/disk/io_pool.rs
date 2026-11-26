use std::{
  convert::identity,
  fs::DirEntry,
  io,
  path::{Path, PathBuf},
  sync::{Arc, Mutex},
  thread::sleep,
  time::Duration,
};

use super::{
  AppendIOHandle, DirHandle, DiskBackend, IOScheduler, OpenFlags, ScanIOHandle,
};
use crate::{
  background::{AsyncTask, TryWaitError},
  metrics::{measure, MetricsRegistry},
  utils::{error, just_drop, ShortenedMutex},
  Error, Result,
};

const RETRY_INTERVAL: Duration = Duration::from_secs(5);
const MAX_RETRY: u8 = 10;

pub enum PendingIO<T: 'static = io::Result<()>> {
  Fulfilled(T),
  Scheduled(AsyncTask<T>),
}
impl<T> PendingIO<T> {
  pub fn wait(self) -> T {
    match self {
      Self::Fulfilled(v) => v,
      Self::Scheduled(o) => o.wait().unwrap(),
    }
  }

  pub fn try_wait(self) -> std::result::Result<T, TryWaitError<Self>> {
    match self {
      Self::Fulfilled(v) => Ok(v),
      Self::Scheduled(task) => task
        .try_wait()
        .map_err(|err| err.map_empty(|pending| Self::Scheduled(pending))),
    }
  }

  pub fn try_park(&self) -> bool {
    match self {
      Self::Fulfilled(_) => false,
      Self::Scheduled(task) => task.try_park(),
    }
  }
}
impl<T> PendingIO<io::Result<T>> {
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
    let base_dir = DirHandle::ensure(base_path, Arc::new(backend), metrics.clone())
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

  pub fn open_append_io(&self, filename: &Path) -> Result<AppendIOHandle> {
    let backend = self
      .base_dir
      .open(OpenFlags::write_only(), filename)
      .wait()
      .unwrap()
      .map_err(Error::IO)?;
    Ok(AppendIOHandle::new(backend))
  }
  pub fn open_scan_io(&self, filename: &Path) -> Result<ScanIOHandle> {
    let backend = self
      .base_dir
      .open(OpenFlags::read_only(), filename)
      .wait()
      .unwrap()
      .map_err(Error::IO)?;
    Ok(ScanIOHandle::new(
      backend,
      self.base_dir.clone(),
      filename.to_path_buf(),
    ))
  }

  fn open_full_with<T, F>(&self, filename: &Path, callback: F) -> AsyncTask<T>
  where
    T: Send + 'static,
    F: FnOnce(io::Result<IOHandle>) -> T + Send + 'static,
  {
    // Direct IO bypasses the OS page cache for predictable latency.
    // To compensate for the lack of OS write buffering, writes are
    // accumulated and sorted in the IO scheduler, then flushed as a
    // single pwritev call per contiguous range.
    let flags = OpenFlags::full();
    let metrics = self.metrics.clone();
    let base_dir = self.base_dir.clone();
    let name = Mutex::new(filename.to_path_buf());
    self.base_dir.open_with(flags, filename, |result| {
      let result = result.map(|backend| IOHandle {
        scheduler: IOScheduler::new(Arc::from(backend)),
        metrics,
        base_dir,
        filename: name,
      });
      callback(result)
    })
  }

  fn open_full(&self, filename: &Path) -> AsyncTask<io::Result<IOHandle>> {
    self.open_full_with(filename, identity)
  }

  pub fn open_dynamic_sized(&self, filename: &Path) -> Result<IOHandle> {
    let mut handle = self
      .open_full(filename)
      .wait()
      .unwrap()
      .map_err(Error::IO)?;
    let len = handle
      .scheduler
      .backend()
      .metadata()
      .map_err(Error::IO)?
      .len();
    handle.scheduler.set_alloc(len);
    Ok(handle)
  }

  pub fn open_static_sized_with<T, F>(&self, filename: &Path, callback: F) -> AsyncTask<T>
  where
    T: Send + 'static,
    F: FnOnce(io::Result<IOHandle>) -> T + Send + 'static,
  {
    self.open_full_with(filename, callback)
  }
  pub fn open_static_sized(&self, filename: &Path) -> AsyncTask<io::Result<IOHandle>> {
    self.open_full(filename)
  }

  pub fn close(&self) {
    self.base_dir.close();
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
/*
 * No I/O callback holds the pool itself, so this never runs on an I/O thread and
 * can always join them.
 */
impl Drop for IOPool {
  fn drop(&mut self) {
    self.close();
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
  scheduler: IOScheduler,
  metrics: Arc<MetricsRegistry>,
  base_dir: Arc<DirHandle>,
  filename: Mutex<PathBuf>,
}
impl IOHandle {
  pub fn read(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
    let _pin = self.scheduler.try_pin().expect("read from a closed handle");
    measure!(
      self.metrics.disk_read,
      self.scheduler.backend().pread_exact(buf, offset)
    )
  }

  pub fn sync_dir_async(&self) -> PendingIO {
    self.base_dir.fdatasync()
  }

  pub fn write_async(&self, buf: &'static [u8], offset: u64) -> PendingIO {
    let Some(pin) = self.scheduler.try_pin_owned() else {
      return PendingIO::Fulfilled(Ok(()));
    };
    let done = self
      .scheduler
      .publish_write(buf, offset, pin, &self.metrics);
    PendingIO::Scheduled(done)
  }

  pub fn write_async_with<R, F>(
    &self,
    buf: &'static [u8],
    offset: u64,
    callback: F,
  ) -> PendingIO<R>
  where
    R: Send + 'static,
    F: FnOnce(io::Result<()>) -> R + Send + 'static,
  {
    let Some(pin) = self.scheduler.try_pin_owned() else {
      return PendingIO::Fulfilled(callback(Ok(())));
    };
    let done =
      self
        .scheduler
        .publish_write_with(buf, offset, pin, &self.metrics, callback);
    PendingIO::Scheduled(done)
  }

  pub fn fdatasync_async(&self) -> PendingIO {
    let Some(pin) = self.scheduler.try_pin_owned() else {
      return PendingIO::Fulfilled(Ok(()));
    };
    let done = self.scheduler.publish_sync(pin, &self.metrics);
    PendingIO::Scheduled(done)
  }

  pub fn fsync_async(&self) -> PendingIO {
    let Some(pin) = self.scheduler.try_pin_owned() else {
      return PendingIO::Fulfilled(Ok(()));
    };
    let done = self.scheduler.backend().fsync_with(just_drop(pin));
    PendingIO::Scheduled(done)
  }

  pub fn fallocate_async(&self, offset: u64, len: u64) -> PendingIO {
    let Some(pin) = self.scheduler.try_pin_owned() else {
      return PendingIO::Fulfilled(Ok(()));
    };
    let done = self
      .scheduler
      .backend()
      .fallocate_with(offset, len, just_drop(pin));
    PendingIO::Scheduled(done)
  }

  pub fn fallocate_then<T, F>(self, offset: u64, len: u64, callback: F) -> PendingIO<T>
  where
    T: Send + 'static,
    F: FnOnce(io::Result<()>, Self) -> T + Send + 'static,
  {
    let Some(pin) = self.scheduler.try_pin_owned() else {
      return PendingIO::Fulfilled(callback(Ok(()), self));
    };
    let backend = self.scheduler.backend().clone();
    let callback = move |result: io::Result<()>| {
      drop(pin);
      callback(result, self)
    };
    let done = backend.fallocate_with(offset, len, callback);
    PendingIO::Scheduled(done)
  }

  /**
   * Remove the file represented by this handle.
   *
   * Requests keep the backend pinned until they complete, so closing first waits
   * out the operations already in flight and rejects any later ones before the
   * file is removed.
   */
  pub fn truncate(&self) -> io::Result<()> {
    self.scheduler.close();
    self.base_dir.remove(&self.filename.l())
  }

  pub fn rename(&self, new_filename: PathBuf) -> io::Result<()> {
    let mut filename = self.filename.l();
    self.base_dir.rename(&filename, &new_filename)?;
    *filename = new_filename;
    Ok(())
  }
}
