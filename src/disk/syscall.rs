use std::{
  fs::{File, OpenOptions},
  io::{IoSlice, Result},
  path::Path,
};

#[cfg(all(unix, not(target_vendor = "apple")))]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::{FileExt, OpenOptionsExt};
#[cfg(unix)]
use std::{
  io::{Error, ErrorKind},
  os::{fd::AsRawFd, unix::fs::FileExt},
};

#[cfg(unix)]
fn retry_with<T>(mut f: impl FnMut() -> Result<T>) -> Result<T> {
  loop {
    match f() {
      Ok(v) => return Ok(v),
      Err(err) if err.kind() == ErrorKind::Interrupted => {}
      Err(err) => return Err(err),
    }
  }
}

#[cfg(unix)]
pub fn pread(file: &File, buf: &mut [u8], offset: u64) -> Result<usize> {
  retry_with(|| file.read_at(buf, offset))
}
#[cfg(windows)]
pub fn pread(file: &File, buf: &mut [u8], offset: u64) -> Result<usize> {
  file.seek_read(buf, offset)
}

#[cfg(unix)]
#[allow(unused)]
pub fn pwrite(file: &File, buf: &[u8], offset: u64) -> Result<usize> {
  retry_with(|| file.write_at(buf, offset))
}
#[cfg(windows)]
pub fn pwrite(file: &File, buf: &[u8], offset: u64) -> Result<usize> {
  file.seek_write(buf, offset)
}

#[cfg(unix)]
#[allow(unused)]
pub fn pwritev(file: &File, bufs: &[IoSlice], offset: u64) -> Result<usize> {
  retry_with(|| unsafe {
    let ret = libc::pwritev(
      file.as_raw_fd(),
      bufs.as_ptr() as *const libc::iovec,
      bufs.len() as libc::c_int,
      offset as _,
    );
    if ret == -1 {
      return Err(Error::last_os_error());
    }
    Ok(ret as usize)
  })
}
#[cfg(windows)]
pub fn pwritev(file: &File, bufs: &[IoSlice], offset: u64) -> Result<usize> {
  let total: usize = bufs.iter().map(|b| b.len()).sum();
  let mut buf = vec![0u8; total];
  let mut pos = 0;
  for slice in bufs {
    buf[pos..slice.len()].copy_from_slice(slice);
    pos += slice.len();
  }
  file.seek_write(&buf, offset)
}

#[cfg(target_os = "linux")]
#[allow(unused)]
pub fn fallocate(file: &File, offset: u64, len: u64) -> Result<()> {
  retry_with(|| unsafe {
    let ret = libc::fallocate(
      file.as_raw_fd(),
      0,
      offset as libc::off_t,
      len as libc::off_t,
    );
    if ret == -1 {
      return Err(Error::last_os_error());
    }
    Ok(())
  })
}
#[cfg(target_vendor = "apple")]
pub fn fallocate(file: &File, offset: u64, len: u64) -> Result<()> {
  if len == 0 {
    return Err(Error::from(ErrorKind::InvalidInput));
  }
  let eof = file.metadata()?.len();
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

  retry_with(|| unsafe {
    let ret = libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut fstore);
    if ret == -1 {
      return Err(Error::last_os_error());
    };
    Ok(())
  })?;
  file.set_len(offset + len)
}
#[cfg(all(not(target_os = "linux"), not(target_vendor = "apple")))]
fn fallocate(file: &File, offset: u64, len: u64) -> Result<()> {
  file.set_len(offset + len)
}

#[cfg(target_vendor = "apple")]
pub fn direct_io(options: &mut OpenOptions, path: &Path) -> Result<File> {
  let file = options.open(path)?;
  let fd = file.as_raw_fd();

  retry_with(|| unsafe {
    let ret = libc::fcntl(fd, libc::F_NOCACHE, 1);
    if ret == -1 {
      return Err(Error::last_os_error());
    }
    Ok(())
  })?;
  Ok(file)
}
#[cfg(all(unix, not(target_vendor = "apple")))]
pub fn direct_io(options: &mut OpenOptions, path: &Path) -> Result<File> {
  options.custom_flags(libc::O_DIRECT).open(path)
}
#[cfg(windows)]
pub fn direct_io(options: &mut OpenOptions, path: &Path) -> Result<File> {
  options
    .custom_flags(winapi::um::winbase::FILE_FLAG_NO_BUFFERING)
    .open(path)
}
