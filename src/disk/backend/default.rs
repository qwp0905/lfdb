use std::{
  fs::{
    create_dir_all, exists, read_dir, remove_file, rename, File, OpenOptions,
    TryLockError,
  },
  io::{IoSlice, Result},
  path::Path,
};

#[cfg(all(unix, not(target_vendor = "apple")))]
use std::os::unix::fs::OpenOptionsExt;

#[cfg(unix)]
use std::{
  io::Error,
  os::{fd::AsRawFd, unix::fs::FileExt},
};

#[cfg(windows)]
use std::{
  os::windows::fs::{FileExt, OpenOptionsExt},
  ptr::copy_nonoverlapping,
};

use super::{DiskBackend, IOBackend};

pub type DefaultDiskBackend = File;
impl IOBackend for DefaultDiskBackend {
  #[cfg(unix)]
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    self.read_at(buf, offset)
  }
  #[cfg(windows)]
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    self.seek_read(buf, offset)
  }

  #[cfg(unix)]
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
    self.write_at(buf, offset)
  }
  #[cfg(windows)]
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
    self.seek_write(buf, offset)
  }

  #[cfg(unix)]
  fn pwritev(&self, bufs: &[IoSlice], offset: u64) -> Result<usize> {
    let ret = unsafe {
      libc::pwritev(
        self.as_raw_fd(),
        bufs.as_ptr() as *const libc::iovec,
        bufs.len() as libc::c_int,
        offset as _,
      )
    };
    if ret == -1 {
      return Err(Error::last_os_error());
    }

    Ok(ret as usize)
  }
  #[cfg(not(unix))]
  fn pwritev(&self, bufs: &[IoSlice], offset: u64) -> Result<usize> {
    let total: usize = bufs.iter().map(|b| b.len()).sum();
    let mut buf = vec![0u8; total];
    let ptr = buf.as_mut_ptr();
    let mut pos = 0;
    for slice in bufs {
      unsafe { copy_nonoverlapping(slice.as_ptr(), ptr.add(pos), slice.len()) };
      pos += slice.len();
    }
    self.seek_write(&buf, offset)
  }

  #[cfg(target_os = "linux")]
  fn fallocate(&self, offset: u64, len: u64) -> Result<()> {
    let ret = unsafe {
      libc::fallocate(
        self.as_raw_fd(),
        0,
        offset as libc::off_t,
        len as libc::off_t,
      )
    };
    if ret == -1 {
      return Err(Error::last_os_error());
    }
    Ok(())
  }
  #[cfg(target_vendor = "apple")]
  fn fallocate(&self, offset: u64, len: u64) -> Result<()> {
    let eof = self.metadata()?.len();
    if eof >= offset + len {
      return Ok(());
    }

    let mut fstore = libc::fstore_t {
      fst_flags: libc::F_ALLOCATEALL,
      fst_posmode: libc::F_PEOFPOSMODE,
      fst_offset: 0,
      fst_length: (offset + len - eof) as libc::off_t,
      fst_bytesalloc: 0,
    };
    let ret = unsafe { libc::fcntl(self.as_raw_fd(), libc::F_PREALLOCATE, &mut fstore) };
    if ret == -1 {
      return Err(Error::last_os_error());
    }
    self.set_len(offset + len)
  }
  #[cfg(all(not(target_os = "linux"), not(target_vendor = "apple")))]
  fn fallocate(&self, offset: u64, len: u64) {
    self.set_len(offset + len)
  }

  fn fsync(&self) -> Result<()> {
    self.sync_all()
  }
  fn fdatasync(&self) -> Result<()> {
    self.sync_data()
  }
  fn metadata(&self) -> Result<std::fs::Metadata> {
    DefaultDiskBackend::metadata(self)
  }
  fn try_flock(&self) -> Result<bool> {
    match self.try_lock() {
      Ok(_) => Ok(true),
      Err(TryLockError::WouldBlock) => Ok(false),
      Err(TryLockError::Error(err)) => Err(err),
    }
  }
  fn unlock(&self) -> Result<()> {
    DefaultDiskBackend::unlock(self)
  }
}

pub struct DefaultIOBackend;
impl DiskBackend for DefaultIOBackend {
  fn open(&self, options: &mut OpenOptions, path: &Path) -> Result<Box<dyn IOBackend>>
  where
    Self: Sized,
  {
    let file = options.open(path)?;
    Ok(Box::new(file))
  }

  #[cfg(target_vendor = "apple")]
  fn open_direct_io(
    &self,
    options: &mut OpenOptions,
    path: &Path,
  ) -> Result<Box<dyn IOBackend>> {
    let file = options.open(path)?;
    let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
    if ret == -1 {
      return Err(Error::last_os_error());
    }
    Ok(Box::new(file))
  }

  #[cfg(all(unix, not(target_vendor = "apple")))]
  fn open_direct_io(
    &self,
    options: &mut OpenOptions,
    path: &Path,
  ) -> Result<Box<dyn IOBackend>> {
    let file = options.custom_flags(libc::O_DIRECT).open(path)?;
    Ok(Box::new(file))
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
}

#[cfg(test)]
#[path = "tests/default.rs"]
mod tests;
