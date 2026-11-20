use std::cell::OnceCell;
use std::fs::{File, OpenOptions};
use std::io::{IoSlice, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
}

#[derive(Clone)]
pub enum TaskType {
  Pwrite {
    offset: u64,
    buf: &'static [u8],
  },
  Pwritev {
    offset: u64,
    bufs: &'static [IoSlice<'static>],
  },
  Fsync,
  Fdatasync,
  Fallocate {
    offset: u64,
    len: u64,
  },
  Open {
    path: PathBuf,
    flags: OpenFlags,
  },
}

pub struct IOContext {
  task_type: TaskType,
  toward: OnceCell<Arc<File>>,
}
impl IOContext {
  pub const fn new(task_type: TaskType) -> Self {
    Self {
      task_type,
      toward: OnceCell::new(),
    }
  }
  pub const fn get_type(&self) -> &'_ TaskType {
    &self.task_type
  }
  pub fn toward(&self) -> &'_ File {
    self.toward.get().unwrap()
  }
  pub fn bind(&self, file: Arc<File>) {
    let _ = self.toward.set(file);
  }
}
