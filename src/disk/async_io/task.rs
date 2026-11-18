use std::cell::OnceCell;
use std::fs::File;
use std::io::IoSlice;
use std::sync::Arc;

#[derive(Clone, Copy)]
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
  pub const fn get_type(&self) -> TaskType {
    self.task_type
  }
  pub fn toward(&self) -> &'_ File {
    self.toward.get().unwrap()
  }
  pub fn bind(&self, file: Arc<File>) {
    let _ = self.toward.set(file);
  }
}
