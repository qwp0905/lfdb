use std::{
  fs::DirEntry,
  io::Result as IOResult,
  path::{Path, PathBuf},
  sync::Arc,
};

use super::{DiskBackend, IOBackend, OpenFlags, PendingIO, SyncScheduler};
use crate::{background::AsyncTask, metrics::MetricsRegistry};

/**
 * Base-directory-bound disk backend.
 *
 * `DirHandle` wraps the `DiskBackend` for namespace operations under one
 * canonical base path, and also keeps an opened directory handle so the pool can
 * lock and sync the directory itself.
 */
pub struct DirHandle {
  scheduler: SyncScheduler,
  disk_backend: Arc<dyn DiskBackend>,
  metrics: Arc<MetricsRegistry>,
  path: PathBuf,
}
impl DirHandle {
  pub fn ensure(
    path: &Path,
    disk_backend: Arc<dyn DiskBackend>,
    metrics: Arc<MetricsRegistry>,
  ) -> IOResult<Self> {
    disk_backend.ensure_dir(path)?;
    let path = path.canonicalize()?;
    let backend = disk_backend
      .open(OpenFlags::directory(), path.to_path_buf())
      .wait()
      .unwrap()
      .map(Arc::<dyn IOBackend>::from)?;

    Ok(Self {
      scheduler: SyncScheduler::new(backend),
      disk_backend,
      metrics,
      path,
    })
  }
  pub fn fdatasync(&self) -> PendingIO {
    PendingIO::Scheduled(self.scheduler.publish(None, &self.metrics))
  }
  pub fn get_path(&self) -> &Path {
    self.path.as_path()
  }
  pub fn read(&self) -> IOResult<Vec<DirEntry>> {
    let mut entries = Vec::new();
    for entry in self.disk_backend.read_dir(&self.path)? {
      entries.push(entry?);
    }
    Ok(entries)
  }
  pub fn remove(&self, filename: &Path) -> IOResult<()> {
    self.disk_backend.remove_file(&self.path.join(filename))
  }
  pub fn exists(&self, filename: &Path) -> IOResult<bool> {
    self.disk_backend.exists(&self.path.join(filename))
  }
  pub fn rename(&self, from: &Path, to: &Path) -> IOResult<()> {
    self
      .disk_backend
      .rename(&self.path.join(from), &self.path.join(to))
  }
  pub fn open_with<T, F>(
    &self,
    flags: OpenFlags,
    filename: &Path,
    callback: F,
  ) -> AsyncTask<T>
  where
    T: Send + 'static,
    F: FnOnce(IOResult<Box<dyn IOBackend>>) -> T + Send + 'static,
  {
    let path = self.path.join(filename);
    self.disk_backend.open_with(flags, path, callback)
  }
  pub fn open(
    &self,
    flags: OpenFlags,
    filename: &Path,
  ) -> AsyncTask<IOResult<Box<dyn IOBackend>>> {
    let path = self.path.join(filename);
    self.disk_backend.open(flags, path)
  }
  pub fn try_lock(&self) -> IOResult<bool> {
    self.scheduler.backend().try_flock()
  }
  pub fn unlock(&self) -> IOResult<()> {
    self.scheduler.backend().unlock()
  }
  pub fn close(&self) {
    self.disk_backend.close();
  }
}
