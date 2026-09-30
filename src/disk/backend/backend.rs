use std::{
  fs::{Metadata, OpenOptions, ReadDir},
  io::{Error, ErrorKind, IoSlice, Result},
  path::Path,
};

use super::super::{AsyncTask, PendingAsync, TaskType};

pub const IO_RETRY: u8 = 3;

type None = fn(&Result<usize>);
pub const NO_CALLBACK: Option<None> = None;

pub struct IOTask {
  pub task_type: TaskType,
  pub done: AsyncTask<usize>,
}
impl IOTask {
  fn new<F: FnOnce(&Result<usize>) + Send + 'static>(
    task_type: TaskType,
    callback: Option<F>,
  ) -> (Self, PendingAsync<usize>) {
    let (task, done) = AsyncTask::new(callback);
    (
      Self {
        task_type,
        done: task,
      },
      done,
    )
  }
  pub fn new_pwrite<F: FnOnce(&Result<usize>) + Send + 'static>(
    buf: &'static [u8],
    offset: u64,
    callback: Option<F>,
  ) -> (Self, PendingAsync<usize>) {
    Self::new(TaskType::Pwrite { offset, buf }, callback)
  }
  pub fn new_pwritev<F: FnOnce(&Result<usize>) + Send + 'static>(
    bufs: &'static [IoSlice<'static>],
    offset: u64,
    callback: Option<F>,
  ) -> (Self, PendingAsync<usize>) {
    Self::new(TaskType::Pwritev { offset, bufs }, callback)
  }
  pub fn new_fsync<F: FnOnce(&Result<usize>) + Send + 'static>(
    callback: Option<F>,
  ) -> (Self, PendingAsync<usize>) {
    Self::new(TaskType::Fsync, callback)
  }
  pub fn new_fdatasync<F: FnOnce(&Result<usize>) + Send + 'static>(
    callback: Option<F>,
  ) -> (Self, PendingAsync<usize>) {
    Self::new(TaskType::Fdatasync, callback)
  }
  pub fn new_fallocate<F: FnOnce(&Result<usize>) + Send + 'static>(
    offset: u64,
    len: u64,
    callback: Option<F>,
  ) -> (Self, PendingAsync<usize>) {
    Self::new(TaskType::Fallocate { offset, len }, callback)
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
  fn batch_submit(&self, tasks: Vec<IOTask>) {
    for task in tasks {
      self.submit(task);
    }
  }
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
