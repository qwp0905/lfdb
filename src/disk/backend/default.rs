/**
 * Default filesystem backend.
 *
 * This implementation uses the standard library whenever it exposes the needed
 * operation. Platform-specific code appears only for operations that are missing
 * from stable std APIs, such as vectored positioned writes, preallocation, and
 * direct-I/O-style open flags.
 */
use std::{
  fs::{
    create_dir_all, exists, read_dir, remove_file, rename, File, OpenOptions,
    TryLockError,
  },
  io::Result,
  path::Path,
  sync::Arc,
};

#[cfg(all(unix, not(target_vendor = "apple")))]
use std::os::unix::fs::OpenOptionsExt;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(windows)]
use std::{
  os::windows::fs::{FileExt, OpenOptionsExt},
  ptr::copy_nonoverlapping,
};

use super::{
  super::{IoSubmitter, Task},
  DiskBackend, IOBackend, IOTask,
};

/**
 * The default `IOBackend` implementation is just `std::fs::File`.
 */
pub struct DefaultIOBackend {
  file: Arc<File>,
  submitter: Arc<IoSubmitter>,
}
impl DefaultIOBackend {
  const fn new(file: Arc<File>, submitter: Arc<IoSubmitter>) -> Self {
    Self { file, submitter }
  }
}
impl IOBackend for DefaultIOBackend {
  #[cfg(unix)]
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    self.file.read_at(buf, offset)
  }
  #[cfg(windows)]
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    self.seek_read(buf, offset)
  }

  fn submit(&self, task: IOTask) -> Result<()> {
    let task = Task {
      toward: self.file.clone(),
      task_type: task.task_type,
      done: task.done,
    };
    self.submitter.submit(task)
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
  submitter: Arc<IoSubmitter>,
}
impl DefaultDiskBackend {
  pub fn new() -> Result<Self> {
    let submitter = IoSubmitter::new(512)?;
    Ok(Self {
      submitter: Arc::new(submitter),
    })
  }
}
impl DiskBackend for DefaultDiskBackend {
  fn open(&self, options: &mut OpenOptions, path: &Path) -> Result<Box<dyn IOBackend>>
  where
    Self: Sized,
  {
    let file = Arc::new(options.open(path)?);
    Ok(Box::new(DefaultIOBackend::new(
      file,
      self.submitter.clone(),
    )))
  }

  #[cfg(target_vendor = "apple")]
  fn open_direct_io(
    &self,
    options: &mut OpenOptions,
    path: &Path,
  ) -> Result<Box<dyn IOBackend>> {
    // macOS does not expose Linux-style O_DIRECT here. F_NOCACHE is the closest
    // default-backend approximation.
    let file = options.open(path)?;
    let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
    if ret == -1 {
      return Err(Error::last_os_error());
    }
    Ok(Box::new(DefaultIOBackend::new(
      Arc::new(file),
      self.submitter.clone(),
    )))
  }

  #[cfg(all(unix, not(target_vendor = "apple")))]
  fn open_direct_io(
    &self,
    options: &mut OpenOptions,
    path: &Path,
  ) -> Result<Box<dyn IOBackend>> {
    // Closest default-backend approximation to Linux O_DIRECT on Windows.
    let file = options.custom_flags(libc::O_DIRECT).open(path)?;
    Ok(Box::new(DefaultIOBackend::new(
      Arc::new(file),
      self.submitter.clone(),
    )))
  }
  #[cfg(windows)]
  fn open_direct_io(
    &self,
    options: &mut OpenOptions,
    path: &Path,
  ) -> Result<Box<dyn IOBackend>> {
    let file = options
      .custom_flags(winapi::um::winbase::FILE_FLAG_NO_BUFFERING)
      .open(path)?;
    Ok(Box::new(file))
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
    self.submitter.close();
  }
}

#[cfg(test)]
#[path = "tests/default.rs"]
mod tests;
