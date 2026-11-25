use std::{
  ffi::CString,
  fs::{Metadata, ReadDir},
  io::{Error, ErrorKind, IoSlice, Result},
  path::{Path, PathBuf},
  sync::Arc,
};

use crate::background::{create_task, create_task_with, AsyncTask, TaskFulfiller};

use super::OpenFlags;

pub const IO_RETRY: u8 = 3;

pub enum IOTask {
  Pwrite {
    offset: u64,
    buf: &'static [u8],
    fulfiller: TaskFulfiller<Result<usize>>,
  },
  Pwritev {
    offset: u64,
    bufs: &'static [IoSlice<'static>],
    fulfiller: TaskFulfiller<Result<usize>>,
  },
  Fsync {
    fulfiller: TaskFulfiller<Result<()>>,
  },
  Fdatasync {
    fulfiller: TaskFulfiller<Result<()>>,
  },
  Fallocate {
    offset: u64,
    len: u64,
    fulfiller: TaskFulfiller<Result<()>>,
  },
}

pub enum DiskTask {
  Open {
    path: CString,
    flags: OpenFlags,
    fulfiller: TaskFulfiller<Result<usize>>,
  },
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
}
impl dyn IOBackend {
  /**
   * Read the entire buffer or fail.
   *
   * Short reads are not accepted by block-oriented callers. The same full read is
   * retried a small number of times and then reported as EOF if it still does not
   * fill the buffer.
   */
  pub fn pread_exact(&self, buf: &mut [u8], offset: u64) -> Result<()> {
    for _ in 0..IO_RETRY {
      if buf.len() == self.pread(buf, offset)? {
        return Ok(());
      }
    }
    Err(Error::from(ErrorKind::UnexpectedEof))
  }

  pub fn pwrite_with<
    T: Send + 'static,
    F: FnOnce(Result<usize>) -> T + Send + 'static,
  >(
    &self,
    buf: &'static [u8],
    offset: u64,
    callback: F,
  ) -> AsyncTask<T> {
    let (fulfiller, done) = create_task_with(callback);
    let task = IOTask::Pwrite {
      offset,
      buf,
      fulfiller,
    };
    self.submit(task);
    done
  }
  pub fn pwrite(&self, buf: &'static [u8], offset: u64) -> AsyncTask<Result<usize>> {
    let (fulfiller, done) = create_task();
    let task = IOTask::Pwrite {
      offset,
      buf,
      fulfiller,
    };
    self.submit(task);
    done
  }
  pub fn pwritev_with<
    T: Send + 'static,
    F: FnOnce(Result<usize>) -> T + Send + 'static,
  >(
    &self,
    bufs: &'static [IoSlice<'static>],
    offset: u64,
    callback: F,
  ) -> AsyncTask<T> {
    let (fulfiller, done) = create_task_with(callback);
    let task = IOTask::Pwritev {
      offset,
      bufs,
      fulfiller,
    };
    self.submit(task);
    done
  }
  pub fn fsync_with<T: Send + 'static, F: FnOnce(Result<()>) -> T + Send + 'static>(
    &self,
    callback: F,
  ) -> AsyncTask<T> {
    let (fulfiller, done) = create_task_with(callback);
    let task = IOTask::Fsync { fulfiller };
    self.submit(task);
    done
  }
  pub fn fsync(&self) -> AsyncTask<Result<()>> {
    let (fulfiller, done) = create_task();
    let task = IOTask::Fsync { fulfiller };
    self.submit(task);
    done
  }
  pub fn fdatasync_with<
    T: Send + 'static,
    F: FnOnce(Result<()>) -> T + Send + 'static,
  >(
    &self,
    callback: F,
  ) -> AsyncTask<T> {
    let (fulfiller, done) = create_task_with(callback);
    let task = IOTask::Fdatasync { fulfiller };
    self.submit(task);
    done
  }
  pub fn fallocate(&self, offset: u64, len: u64) -> AsyncTask<Result<()>> {
    let (fulfiller, done) = create_task();
    let task = IOTask::Fallocate {
      offset,
      len,
      fulfiller,
    };
    self.submit(task);
    done
  }
  pub fn fallocate_with<
    T: Send + 'static,
    F: FnOnce(Result<()>) -> T + Send + 'static,
  >(
    &self,
    offset: u64,
    len: u64,
    callback: F,
  ) -> AsyncTask<T> {
    let (fulfiller, done) = create_task_with(callback);
    let task = IOTask::Fallocate {
      offset,
      len,
      fulfiller,
    };
    self.submit(task);
    done
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
  fn submit(&self, task: DiskTask);
  /**
   * # Safety
   * `raw` must come from an open task this backend received through `submit`,
   * and be adopted only once, since it carries ownership of the opened handle.
   */
  unsafe fn adopt(&self, raw: usize) -> Box<dyn IOBackend>;
  fn read_dir(&self, path: &Path) -> Result<ReadDir>;
  fn remove_file(&self, path: &Path) -> Result<()>;
  fn exists(&self, path: &Path) -> Result<bool>;
  fn rename(&self, from: &Path, to: &Path) -> Result<()>;
  fn ensure_dir(&self, path: &Path) -> Result<()>;
  /**
   * Joins the I/O threads, so the owner must call it from outside them. Dropping
   * is not enough: an I/O callback can hold the last reference.
   */
  fn close(&self);
}

impl dyn DiskBackend {
  pub fn open(
    self: &Arc<Self>,
    flags: OpenFlags,
    path: PathBuf,
  ) -> AsyncTask<Result<Box<dyn IOBackend>>> {
    let this = self.clone();
    // SAFETY: the task is submitted to this backend right below, and its callback runs once.
    let callback = move |result: Result<_>| unsafe { result.map(|raw| this.adopt(raw)) };

    let path = CString::new(path.into_os_string().into_encoded_bytes())
      .expect("path must not contains nul string.");
    let (fulfiller, done) = create_task_with(callback);
    let task = DiskTask::Open {
      path,
      flags,
      fulfiller,
    };
    self.submit(task);
    done
  }
  pub fn open_with<T, F>(
    self: &Arc<Self>,
    flags: OpenFlags,
    path: PathBuf,
    callback: F,
  ) -> AsyncTask<T>
  where
    T: Send + 'static,
    F: FnOnce(Result<Box<dyn IOBackend>>) -> T + Send + 'static,
  {
    let this = self.clone();
    // SAFETY: the task is submitted to this backend right below, and its callback runs once.
    let callback =
      move |result: Result<_>| callback(unsafe { result.map(|raw| this.adopt(raw)) });

    let path = CString::new(path.into_os_string().into_encoded_bytes())
      .expect("path must not contains nul string.");
    let (fulfiller, done) = create_task_with(callback);
    let task = DiskTask::Open {
      path,
      flags,
      fulfiller,
    };
    self.submit(task);
    done
  }
}
