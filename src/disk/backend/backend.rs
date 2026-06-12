use std::{
  fs::{Metadata, OpenOptions, ReadDir},
  io::{Error, ErrorKind, IoSlice, Read, Result, Write},
  panic::RefUnwindSafe,
  path::Path,
};

pub trait IOBackend: Send + Sync + RefUnwindSafe + Read + Write + 'static {
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize>;
  fn pwritev(&self, bufs: &[IoSlice], offset: u64) -> Result<usize>;
  fn fallocate(&self, offset: u64, len: u64) -> Result<()>;
  fn fsync(&self) -> Result<()>;
  fn fdatasync(&self) -> Result<()>;
  fn metadata(&self) -> Result<Metadata>;
  fn try_flock(&self) -> Result<bool>;
  fn unlock(&self) -> Result<()>;

  fn pread_exact(&self, mut buf: &mut [u8], mut offset: u64) -> Result<()> {
    while !buf.is_empty() {
      match self.pread(buf, offset) {
        Ok(0) => return Err(Error::from(ErrorKind::UnexpectedEof)),
        Ok(n) => {
          let tmp = buf;
          buf = &mut tmp[n..];
          offset += n as u64;
        }
        Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
        Err(e) => return Err(e),
      }
    }

    Ok(())
  }

  fn pwrite_all(&self, mut buf: &[u8], mut offset: u64) -> Result<()> {
    while !buf.is_empty() {
      match self.pwrite(buf, offset) {
        Ok(0) => return Err(Error::from(ErrorKind::WriteZero)),
        Ok(n) => {
          buf = &buf[n..];
          offset += n as u64;
        }
        Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
        Err(e) => return Err(e),
      }
    }
    Ok(())
  }

  fn pwritev_all(&self, mut bufs: &mut [IoSlice<'_>], mut offset: u64) -> Result<()> {
    while !bufs.is_empty() {
      match self.pwritev(bufs, offset) {
        Ok(0) => {
          return Err(Error::new(ErrorKind::WriteZero, "pwritev wrote zero bytes"))
        }
        Ok(n) => {
          IoSlice::advance_slices(&mut bufs, n);
          offset += n as u64;
        }
        Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
        Err(e) => return Err(e),
      }
    }

    Ok(())
  }
}

pub trait DiskBackend: Send + Sync + RefUnwindSafe + 'static {
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
