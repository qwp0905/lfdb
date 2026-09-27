use std::{
  fs::{Metadata, OpenOptions, ReadDir},
  io::{Error, ErrorKind, IoSlice, Result},
  path::Path,
};

use crate::background::{oneshot, Oneshot, OneshotFulfill};

use super::super::TaskType;

const RETRY: u8 = 3;

pub struct IOTask {
  pub task_type: TaskType,
  pub done: OneshotFulfill<Result<usize>>,
}
impl IOTask {
  pub const fn new(task_type: TaskType, done: OneshotFulfill<Result<usize>>) -> Self {
    Self { task_type, done }
  }
  pub fn new_pwrite(buf: &'static [u8], offset: u64) -> (Self, Oneshot<Result<usize>>) {
    let (o, f) = oneshot();
    (Self::new(TaskType::Pwrite { offset, buf }, f), o)
  }
  pub fn new_pwritev(
    bufs: &'static [IoSlice<'static>],
    offset: u64,
  ) -> (Self, Oneshot<Result<usize>>) {
    let (o, f) = oneshot();
    (Self::new(TaskType::Pwritev { offset, bufs }, f), o)
  }
  pub fn new_fsync() -> (Self, Oneshot<Result<usize>>) {
    let (o, f) = oneshot();
    (Self::new(TaskType::Fsync, f), o)
  }
  pub fn new_fdatasync() -> (Self, Oneshot<Result<usize>>) {
    let (o, f) = oneshot();
    (Self::new(TaskType::Fdatasync, f), o)
  }
  pub fn new_fallocate(offset: u64, len: u64) -> (Self, Oneshot<Result<usize>>) {
    let (o, f) = oneshot();
    (Self::new(TaskType::Fallocate { offset, len }, f), o)
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
  fn submit(&self, task: IOTask) -> Result<()>;
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
    for _ in 0..RETRY {
      if buf.len() == self.pread(buf, offset)? {
        return Ok(());
      }
    }
    Err(Error::from(ErrorKind::UnexpectedEof))
  }

  fn submit_pwrite(
    &self,
    buf: &'static [u8],
    offset: u64,
  ) -> Result<Oneshot<Result<usize>>> {
    let (task, done) = IOTask::new_pwrite(buf, offset);
    self.submit(task)?;
    Ok(done)
  }
  fn submit_pwritev(
    &self,
    bufs: &'static [IoSlice<'static>],
    offset: u64,
  ) -> Result<Oneshot<Result<usize>>> {
    let (task, done) = IOTask::new_pwritev(bufs, offset);
    self.submit(task)?;
    Ok(done)
  }
  fn submit_fsync(&self) -> Result<Oneshot<Result<usize>>> {
    let (task, done) = IOTask::new_fsync();
    self.submit(task)?;
    Ok(done)
  }
  fn submit_fdatasync(&self) -> Result<Oneshot<Result<usize>>> {
    let (task, done) = IOTask::new_fdatasync();
    self.submit(task)?;
    Ok(done)
  }
  fn submit_fallocate(&self, offset: u64, len: u64) -> Result<Oneshot<Result<usize>>> {
    let (task, done) = IOTask::new_fallocate(offset, len);
    self.submit(task)?;
    Ok(done)
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
  fn close(&self);
}
