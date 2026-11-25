use std::{
  ffi::CString,
  fs::File,
  io::{IoSlice, Result},
  sync::Arc,
};
#[cfg(not(target_os = "linux"))]
use std::{fs::OpenOptions, path::Path};

use crate::background::TaskFulfiller;

#[cfg(not(target_os = "linux"))]
use super::super::direct_io;

#[derive(Clone)]
pub struct OpenFlags {
  read: bool,
  write: bool,
  create: bool,
  direct: bool,
}
impl OpenFlags {
  pub const fn directory() -> Self {
    Self {
      read: true,
      write: false,
      create: false,
      direct: false,
    }
  }
  pub const fn read_only() -> Self {
    Self {
      read: true,
      write: false,
      create: false,
      direct: true,
    }
  }
  pub const fn write_only() -> Self {
    Self {
      read: false,
      write: true,
      create: true,
      direct: true,
    }
  }
  pub const fn full() -> Self {
    Self {
      read: true,
      write: true,
      create: true,
      direct: true,
    }
  }
  #[cfg(test)]
  pub const fn buffered() -> Self {
    Self {
      read: true,
      write: true,
      create: true,
      direct: false,
    }
  }

  #[cfg(not(target_os = "linux"))]
  pub fn open(&self, path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options
      .create(self.create)
      .read(self.read)
      .write(self.write);
    if !self.direct {
      return options.open(path);
    }
    direct_io(&mut options, path)
  }

  #[cfg(target_os = "linux")]
  pub fn as_raw(&self) -> i32 {
    let access = match (self.read, self.write) {
      (true, false) => libc::O_RDONLY,
      (false, true) => libc::O_WRONLY,
      _ => libc::O_RDWR,
    };
    let create = if self.create { libc::O_CREAT } else { 0 };
    let direct = if self.direct { libc::O_DIRECT } else { 0 };
    libc::O_CLOEXEC | access | create | direct
  }
}

pub enum BoundIOTask {
  Pwrite {
    offset: u64,
    buf: &'static [u8],
    file: Arc<File>,
    fulfiller: TaskFulfiller<Result<usize>>,
  },
  Pwritev {
    offset: u64,
    bufs: &'static [IoSlice<'static>],
    file: Arc<File>,
    fulfiller: TaskFulfiller<Result<usize>>,
  },
  Fsync {
    file: Arc<File>,
    fulfiller: TaskFulfiller<Result<()>>,
  },
  Fdatasync {
    file: Arc<File>,
    fulfiller: TaskFulfiller<Result<()>>,
  },
  Fallocate {
    offset: u64,
    len: u64,
    file: Arc<File>,
    fulfiller: TaskFulfiller<Result<()>>,
  },
  Open {
    path: CString,
    flags: OpenFlags,
    fulfiller: TaskFulfiller<Result<usize>>,
  },
}
