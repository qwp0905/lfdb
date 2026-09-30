use std::{
  fs::File,
  io::{IoSlice, Result},
  num::NonZero,
  thread::available_parallelism,
};

use crate::background::{Close, SharedWorkThread, ThreadBuilder};

use super::{FullTask, TaskType};

#[cfg(unix)]
use std::{
  io::Error,
  os::{fd::AsRawFd, unix::fs::FileExt},
};

#[cfg(target_vendor = "apple")]
use std::io::ErrorKind;
#[cfg(all(unix, not(target_vendor = "apple")))]
use std::os::unix::fs::OpenOptionsExt;

#[cfg(windows)]
use std::{
  os::windows::fs::{FileExt, OpenOptionsExt},
  ptr::copy_nonoverlapping,
};

#[cfg(unix)]
fn pwrite(file: &File, buf: &[u8], offset: u64) -> Result<usize> {
  file.write_at(buf, offset)
}
#[cfg(windows)]
fn pwrite(file: &File, buf: &[u8], offset: u64) -> Result<usize> {
  self.seek_write(buf, offset)
}
#[cfg(unix)]
fn pwritev(file: &File, bufs: &[IoSlice], offset: u64) -> Result<usize> {
  let ret = unsafe {
    libc::pwritev(
      file.as_raw_fd(),
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
fn pwritev(file: &File, bufs: &[IoSlice], offset: u64) -> Result<usize> {
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
fn fallocate(file: &File, offset: u64, len: u64) -> Result<()> {
  let ret = unsafe {
    libc::fallocate(
      file.as_raw_fd(),
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
fn fallocate(file: &File, offset: u64, len: u64) -> Result<()> {
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
  let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut fstore) };
  if ret == -1 {
    return Err(Error::last_os_error());
  }
  file.set_len(offset + len)
}
#[cfg(all(not(target_os = "linux"), not(target_vendor = "apple")))]
fn fallocate(file: &File, offset: u64, len: u64) -> Result<()> {
  file.set_len(offset + len)
}

fn handle_task(task: FullTask) {
  let file = task.toward;
  let result = match task.task_type {
    TaskType::Pwrite { offset, buf } => pwrite(&file, buf, offset),
    TaskType::Pwritev { offset, bufs } => pwritev(&file, bufs, offset),
    TaskType::Fsync => file.sync_all().map(|_| 0),
    TaskType::Fdatasync => file.sync_data().map(|_| 0),
    TaskType::Fallocate { offset, len } => fallocate(&file, offset, len).map(|_| 0),
  };
  task.done.fulfill(result);
}
pub struct AsyncIO {
  thread: SharedWorkThread<FullTask, ()>,
}
impl AsyncIO {
  pub fn new(_: u32) -> Result<Self> {
    let count = available_parallelism()
      .unwrap_or(unsafe { NonZero::new_unchecked(1) })
      .get();
    let thread = ThreadBuilder::new()
      .name("async io")
      .multi(count)
      .shared(handle_task);
    Ok(Self { thread })
  }

  pub fn submit(&self, task: FullTask) {
    self.thread.dispatch(task);
  }

  pub fn close(&self) {
    self.thread.close();
  }
}
