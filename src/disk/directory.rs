use std::{
  fs::{DirEntry, OpenOptions},
  io::Result as IOResult,
  path::{Path, PathBuf},
  sync::Arc,
};

use super::{DiskBackend, IOBackend, PendingIO, SyncScheduler};
use crate::metrics::MetricsRegistry;

/**
 * Base-directory-bound disk backend.
 *
 * `DirHandle` wraps the `DiskBackend` for namespace operations under one
 * canonical base path, and also keeps an opened directory handle so the pool can
 * lock and sync the directory itself.
 */
pub struct DirHandle {
  scheduler: SyncScheduler,
  disk_backend: Box<dyn DiskBackend>,
  metrics: Arc<MetricsRegistry>,
  path: PathBuf,
}
impl DirHandle {
  pub fn ensure(
    path: &Path,
    disk_backend: Box<dyn DiskBackend>,
    metrics: Arc<MetricsRegistry>,
  ) -> IOResult<Self> {
    let mut options = OpenOptions::new();
    disk_backend.ensure_dir(path)?;
    let path = path.canonicalize()?;
    let backend = disk_backend
      .open(options.read(true), &path)
      .map(Arc::<dyn IOBackend>::from)?;

    Ok(Self {
      scheduler: SyncScheduler::new(backend),
      disk_backend,
      metrics,
      path,
    })
  }
  pub fn fdatasync(&self) -> PendingIO {
    if self.scheduler.is_closed() {
      return PendingIO::Fulfilled(Ok(()));
    }
    let done = self.scheduler.publish(&self.metrics);
    PendingIO::Scheduled(done)
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
  pub fn open_direct_io(
    &self,
    options: &mut OpenOptions,
    path: &Path,
  ) -> IOResult<Box<dyn IOBackend>> {
    self.disk_backend.open_direct_io(options, path)
  }
  pub fn try_lock(&self) -> IOResult<bool> {
    self.scheduler.backend().try_flock()
  }
  pub fn unlock(&self) -> IOResult<()> {
    self.scheduler.backend().unlock()
  }
}
