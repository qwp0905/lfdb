use std::{
  fs::{File, OpenOptions},
  io::{IoSlice, /* IoSliceMut,  */ Result},
  path::Path,
};

#[cfg(any(
  target_os = "dragonfly",
  target_os = "freebsd",
  target_os = "netbsd",
  target_os = "openbsd",
  target_vendor = "apple",
  target_os = "cygwin",
))]
pub const fn max_iov() -> usize {
  libc::IOV_MAX as usize
}
#[cfg(any(
  target_os = "android",
  target_os = "emscripten",
  target_os = "linux",
  target_os = "nto",
))]
pub const fn max_iov() -> usize {
  libc::UIO_MAXIOV as usize
}
#[cfg(not(any(
  target_os = "android",
  target_os = "dragonfly",
  target_os = "emscripten",
  target_os = "espidf",
  target_os = "freebsd",
  target_os = "linux",
  target_os = "netbsd",
  target_os = "nuttx",
  target_os = "nto",
  target_os = "openbsd",
  target_os = "horizon",
  target_os = "vita",
  target_vendor = "apple",
  target_os = "cygwin",
)))]
pub const fn max_iov() -> usize {
  16 // The minimum value required by POSIX.
}

#[cfg(unix)]
use libc;

#[cfg(all(unix, not(target_vendor = "apple")))]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(unix)]
use std::{
  io::Error,
  os::{fd::AsRawFd, unix::fs::FileExt},
};

#[cfg(windows)]
use winapi;

#[cfg(windows)]
use std::{
  intrinsics::copy_nonoverlapping,
  os::windows::fs::{FileExt, OpenOptionsExt},
};

pub trait Pread {
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
}
impl Pread for File {
  #[cfg(unix)]
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    self.read_at(buf, offset)
  }

  #[cfg(windows)]
  fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    self.seek_read(buf, offset)
  }
}
pub trait Pwrite {
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize>;
}
impl Pwrite for File {
  #[cfg(unix)]
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
    self.write_at(buf, offset)
  }

  #[cfg(windows)]
  fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
    self.seek_write(buf, offset)
  }
}

// pub trait Preadv {
//   fn preadv(&self, bufs: &mut [IoSliceMut], offset: u64) -> Result<usize>;
// }
// impl Preadv for File {
//   #[cfg(unix)]
//   fn preadv(&self, bufs: &mut [IoSliceMut], offset: u64) -> Result<usize> {
//     let ret = unsafe {
//       libc::preadv(
//         self.as_raw_fd(),
//         bufs.as_mut_ptr() as *mut libc::iovec as *const libc::iovec,
//         bufs.len() as libc::c_int,
//         offset as _,
//       )
//     };
//     if ret == -1 {
//       return Err(Error::last_os_error());
//     }

//     Ok(ret as usize)
//   }
//   #[cfg(windows)]
//   fn preadv(&self, bufs: &mut [IoSliceMut], offset: u64) -> Result<usize> {
//     let total: usize = bufs.iter().map(|b| b.len()).sum();
//     let mut buf = vec![0u8; total];
//     let ret = self.seek_read(&mut buf, offset)?;
//     let mut pos = 0;
//     let ptr = buf.as_ptr();
//     for slice in bufs {
//       unsafe { copy_nonoverlapping(ptr.add(pos), slice.as_mut_ptr(), slice.len()) };
//       pos += slice.len();
//     }
//     Ok(ret)
//   }
// }

pub trait Pwritev {
  fn pwritev(&self, bufs: &[IoSlice], offset: u64) -> Result<usize>;
}
impl Pwritev for File {
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
  #[cfg(windows)]
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
}

pub trait DirectIO {
  fn direct_io<P: AsRef<Path>>(&mut self, path: P) -> Result<File>;
}

impl DirectIO for OpenOptions {
  #[cfg(target_vendor = "apple")]
  fn direct_io<P: AsRef<Path>>(&mut self, path: P) -> Result<File> {
    let file = self.open(path)?;
    let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
    if ret == -1 {
      return Err(Error::last_os_error());
    }
    Ok(file)
  }
  #[cfg(all(unix, not(target_vendor = "apple")))]
  fn direct_io<P: AsRef<Path>>(&mut self, path: P) -> Result<File> {
    self.custom_flags(libc::O_DIRECT).open(path)
  }
  #[cfg(windows)]
  fn direct_io<P: AsRef<Path>>(&mut self, path: P) -> Result<File> {
    self
      .custom_flags(winapi::um::winbase::FILE_FLAG_NO_BUFFERING)
      .open(path)
  }
}

pub trait Fallocate {
  fn fallocate(&self, offset: u64, len: u64) -> Result<()>;
}
impl Fallocate for File {
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
    let mut fstore = libc::fstore_t {
      fst_flags: libc::F_ALLOCATEALL,
      fst_posmode: libc::F_PEOFPOSMODE,
      fst_offset: offset as libc::off_t,
      fst_length: len as libc::off_t,
      fst_bytesalloc: 0,
    };
    let ret = unsafe { libc::fcntl(self.as_raw_fd(), libc::F_PREALLOCATE, &mut fstore) };
    if ret == -1 {
      return Err(Error::last_os_error());
    }
    Ok(())
  }
  #[cfg(all(not(target_os = "linux"), not(target_vendor = "apple")))]
  fn fallocate(&self, _offset: u64, _len: u64) {}
}

#[cfg(test)]
#[path = "tests/io.rs"]
mod tests;
