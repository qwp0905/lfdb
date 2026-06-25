use std::{
  fs::{Metadata, OpenOptions, ReadDir},
  io::{Error, ErrorKind, IoSlice, Read, Result, Write},
  path::Path,
};

const RETRY: u8 = 3;

/**
 * Low-level operations for one opened filesystem object.
 *
 * This trait is modeled after the operation set available on a single Linux
 * file descriptor: positioned reads and writes, vectored writes, allocation,
 * durability calls, metadata access, and advisory locking. Other platform
 * backends provide the closest matching behavior, and test backends can use the
 * same interface to inject I/O faults.
 */
pub trait IOBackend: Send + Sync + Read + Write {
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize>;
  fn pwritev(&self, bufs: &[IoSlice], offset: u64) -> Result<usize>;
  fn fallocate(&self, offset: u64, len: u64) -> Result<()>;
  fn fsync(&self) -> Result<()>;
  fn fdatasync(&self) -> Result<()>;
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
  fn pread_or_fail(&self, buf: &mut [u8], offset: u64) -> Result<()> {
    for _ in 0..RETRY {
      if buf.len() == self.pread(buf, offset)? {
        return Ok(());
      }
    }
    Err(Error::from(ErrorKind::UnexpectedEof))
  }
  /**
   * Write the entire buffer or fail.
   *
   * This is not `write_all`: a short write is not treated as partial progress,
   * and the helper never advances the offset to write only the remaining suffix.
   * For block/direct-I/O paths, the submitted buffer must complete as one logical
   * operation; otherwise the whole request is retried a small number of times and
   * then reported as failed.
   */
  fn pwrite_or_fail(&self, buf: &[u8], offset: u64) -> Result<()> {
    for _ in 0..RETRY {
      if buf.len() == self.pwrite(buf, offset)? {
        return Ok(());
      }
    }
    Err(Error::from(ErrorKind::WriteZero))
  }
  /**
   * Write the entire vectored batch or fail.
   *
   * A short vectored write is not treated as progress through the batch. The same
   * full batch must complete as one logical operation, or the helper reports
   * failure after a small number of retries.
   */
  fn pwritev_or_fail(&self, bufs: &[IoSlice<'_>], offset: u64) -> Result<()> {
    let total: usize = bufs.iter().map(|b| b.len()).sum();
    for _ in 0..RETRY {
      if total == self.pwritev(bufs, offset)? {
        return Ok(());
      }
    }
    Err(Error::from(ErrorKind::WriteZero))
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
