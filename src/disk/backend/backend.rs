use std::{
  fs::{Metadata, OpenOptions, ReadDir},
  io::{Error, ErrorKind, IoSlice, Result},
  path::Path,
};

use crate::background::{
  create_tagged_task, create_tagged_task_with, AsyncTask, TaskFulfiller,
};

use super::{IOContext, TaskType};

pub const IO_RETRY: u8 = 3;

pub struct IOTask {
  fulfiller: TaskFulfiller<Result<usize>, IOContext>,
}

impl IOTask {
  pub fn fulfill(self, result: Result<usize>) {
    self.fulfiller.fulfill(result);
  }
  pub fn context(&self) -> &IOContext {
    self.fulfiller.context()
  }
  const fn new(fulfiller: TaskFulfiller<Result<usize>, IOContext>) -> Self {
    Self { fulfiller }
  }
  #[cfg(target_os = "linux")]
  pub(crate) fn into_raw(this: Self) -> *mut () {
    TaskFulfiller::into_raw(this.fulfiller)
  }
  #[cfg(target_os = "linux")]
  pub(crate) const unsafe fn from_raw(raw: *mut ()) -> Self {
    Self::new(unsafe { TaskFulfiller::from_raw(raw) })
  }
  fn create_with<T: Send + 'static, F: FnOnce(Result<usize>) -> T + Send + 'static>(
    task_type: TaskType,
    callback: F,
  ) -> (Self, AsyncTask<T>) {
    let (fulfiller, done) = create_tagged_task_with(IOContext::new(task_type), callback);
    (Self::new(fulfiller), done)
  }
  fn create(task_type: TaskType) -> (Self, AsyncTask<Result<usize>>) {
    let (fulfiller, done) = create_tagged_task(IOContext::new(task_type));
    (Self::new(fulfiller), done)
  }
  pub fn new_pwrite_with<
    T: Send + 'static,
    F: FnOnce(Result<usize>) -> T + Send + 'static,
  >(
    buf: &'static [u8],
    offset: u64,
    callback: F,
  ) -> (Self, AsyncTask<T>) {
    Self::create_with(TaskType::Pwrite { offset, buf }, callback)
  }
  pub fn new_pwrite(buf: &'static [u8], offset: u64) -> (Self, AsyncTask<Result<usize>>) {
    Self::create(TaskType::Pwrite { offset, buf })
  }
  pub fn new_pwritev_with<
    T: Send + 'static,
    F: FnOnce(Result<usize>) -> T + Send + 'static,
  >(
    bufs: &'static [IoSlice<'static>],
    offset: u64,
    callback: F,
  ) -> (Self, AsyncTask<T>) {
    Self::create_with(TaskType::Pwritev { offset, bufs }, callback)
  }
  pub fn new_fsync_with<
    T: Send + 'static,
    F: FnOnce(Result<usize>) -> T + Send + 'static,
  >(
    callback: F,
  ) -> (Self, AsyncTask<T>) {
    Self::create_with(TaskType::Fsync, callback)
  }
  pub fn new_fsync() -> (Self, AsyncTask<Result<usize>>) {
    Self::create(TaskType::Fsync)
  }
  pub fn new_fdatasync_with<
    T: Send + 'static,
    F: FnOnce(Result<usize>) -> T + Send + 'static,
  >(
    callback: F,
  ) -> (Self, AsyncTask<T>) {
    Self::create_with(TaskType::Fdatasync, callback)
  }
  pub fn new_fallocate_with<
    T: Send + 'static,
    F: FnOnce(Result<usize>) -> T + Send + 'static,
  >(
    offset: u64,
    len: u64,
    callback: F,
  ) -> (Self, AsyncTask<T>) {
    Self::create_with(TaskType::Fallocate { offset, len }, callback)
  }
  pub fn new_fallocate(offset: u64, len: u64) -> (Self, AsyncTask<Result<usize>>) {
    Self::create(TaskType::Fallocate { offset, len })
  }
}

/**
 * Low-level operations for one opened filesystem object.
 *
 * This trait is modeled after the operation set available on a single Linux
 * file descriptor: positioned reads and writes, vectored writes, allocation,
 * durability calls, metadata access, and advisory locking. Other platform
 * backends provide the closest matching behavior, and test backends can use the
 * same interface to inject I/O faults.
 */
pub trait IOBackend: Send + Sync {
  fn submit(&self, task: IOTask);
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
  fn metadata(&self) -> Result<Metadata>;
  fn try_flock(&self) -> Result<bool>;
  fn unlock(&self) -> Result<()>;

  /**
   * Read the entire buffer or fail.
   *
   * Short reads are not accepted by block-oriented callers. The same full read is
   * retried a small number of times and then reported as EOF if it still does not
   * fill the buffer.
   */
  fn pread_exact(&self, buf: &mut [u8], offset: u64) -> Result<()> {
    for _ in 0..IO_RETRY {
      if buf.len() == self.pread(buf, offset)? {
        return Ok(());
      }
    }
    Err(Error::from(ErrorKind::UnexpectedEof))
  }
}

/**
 * Low-level filesystem namespace backend and `IOBackend` factory.
 *
 * `IOBackend` abstracts operations on one opened handle. `DiskBackend` abstracts
 * the surrounding filesystem namespace: opening handles, listing directories,
 * creating directories, renaming paths, removing files, and checking path
 * existence. Like `IOBackend`, it follows the Linux filesystem model first and
 * lets other platforms provide the closest practical behavior.
 */
pub trait DiskBackend: Send + Sync {
  fn open(&self, options: &mut OpenOptions, path: &Path) -> Result<Box<dyn IOBackend>>;
  fn open_direct_io(
    &self,
    options: &mut OpenOptions,
    path: &Path,
  ) -> Result<Box<dyn IOBackend>>;
  fn read_dir(&self, path: &Path) -> Result<ReadDir>;
  fn remove_file(&self, path: &Path) -> Result<()>;
  fn exists(&self, path: &Path) -> Result<bool>;
  fn rename(&self, from: &Path, to: &Path) -> Result<()>;
  fn ensure_dir(&self, path: &Path) -> Result<()>;
}
