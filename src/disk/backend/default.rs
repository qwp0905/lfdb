use std::{
  fs::{create_dir_all, exists, read_dir, remove_file, rename, File, TryLockError},
  io::Result,
  path::Path,
  sync::Arc,
};

use super::{from_raw, pread, AsyncIO, DiskBackend, IOBackend, IOTask};

/**
 * The default `IOBackend` implementation is just `std::fs::File`.
 */
pub struct DefaultIOBackend {
  file: Arc<File>,
  async_io: Arc<AsyncIO>,
}
impl DefaultIOBackend {
  const fn new(file: Arc<File>, async_io: Arc<AsyncIO>) -> Self {
    Self { file, async_io }
  }
}
impl IOBackend for DefaultIOBackend {
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    pread(&self.file, buf, offset)
  }

  fn submit(&self, task: IOTask) {
    task.context().bind(self.file.clone());
    self.async_io.submit(task);
  }

  fn metadata(&self) -> Result<std::fs::Metadata> {
    self.file.metadata()
  }
  fn try_flock(&self) -> Result<bool> {
    match self.file.try_lock() {
      Ok(_) => Ok(true),
      Err(TryLockError::WouldBlock) => Ok(false),
      Err(TryLockError::Error(err)) => Err(err),
    }
  }
  fn unlock(&self) -> Result<()> {
    self.file.unlock()
  }
}

/**
 * Filesystem namespace operations implemented with the standard library.
 */
pub struct DefaultDiskBackend {
  async_io: Arc<AsyncIO>,
}
impl DefaultDiskBackend {
  pub fn new() -> Result<Self> {
    let async_io = AsyncIO::new(128)?;
    Ok(Self {
      async_io: Arc::new(async_io),
    })
  }
}
impl DiskBackend for DefaultDiskBackend {
  fn submit_open(&self, task: IOTask) {
    self.async_io.submit(task);
  }
  unsafe fn adopt(&self, raw: usize) -> Box<dyn IOBackend> {
    let file = Arc::new(from_raw(raw));
    Box::new(DefaultIOBackend::new(file, self.async_io.clone()))
  }
  fn read_dir(&self, path: &Path) -> Result<std::fs::ReadDir> {
    read_dir(path)
  }
  fn remove_file(&self, path: &Path) -> Result<()> {
    remove_file(path)
  }

  fn exists(&self, path: &Path) -> Result<bool> {
    exists(path)
  }
  fn rename(&self, from: &Path, to: &Path) -> Result<()> {
    rename(from, to)
  }

  fn ensure_dir(&self, path: &Path) -> Result<()> {
    create_dir_all(path)
  }

  fn close(&self) {
    self.async_io.close();
  }
}

#[cfg(all(test, not(loom)))]
#[path = "tests/default.rs"]
mod tests;
