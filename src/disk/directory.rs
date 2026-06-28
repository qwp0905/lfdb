use std::{
  fs::{DirEntry, OpenOptions},
  io::Result as IOResult,
  path::{Path, PathBuf},
  sync::Arc,
};

use crate::{background::Oneshot, utils::SBox};

use super::{DiskBackend, HandleState, IOBackend, IOThread, TaskPublisher};

/**
 * Base-directory-bound disk backend.
 *
 * `DirHandle` wraps the `DiskBackend` for namespace operations under one
 * canonical base path, and also keeps an opened directory handle so the pool can
 * lock and sync the directory itself.
 */
pub struct DirHandle {
  io_backend: Arc<dyn IOBackend>,
  disk_backend: Box<dyn DiskBackend>,
  sync_handle: SBox<TaskPublisher<()>>,
  thread: Arc<IOThread>,
  state: SBox<HandleState>,
  path: PathBuf,
}
impl DirHandle {
  pub fn ensure(
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
  pub fn fdatasync(&self) -> Oneshot<IOResult<()>> {
    self
      .sync_handle
      .publish_sync(&self.state, &*self.thread, &self.io_backend)
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
    self.io_backend.try_flock()
  }
  pub fn unlock(&self) -> IOResult<()> {
    self.io_backend.unlock()
  }
}
