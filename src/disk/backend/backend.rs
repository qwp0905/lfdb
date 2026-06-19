use std::{
  fs::{Metadata, OpenOptions, ReadDir},
  io::{Error, ErrorKind, IoSlice, Read, Result, Write},
  path::Path,
};

const RETRY: u8 = 3;

pub trait IOBackend: Send + Sync + Read + Write + 'static {
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
   * avoid half read.
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
   * avoid half read.
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
   * avoid half read.
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

pub trait DiskBackend: Send + Sync + 'static {
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
