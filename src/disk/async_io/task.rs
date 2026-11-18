use std::fs::File;
use std::io::{IoSlice, Result};
use std::sync::Arc;

use crate::background::TaskFulfiller;

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

pub struct AsyncIOTask<T> {
  task_type: TaskType,
  toward: Arc<File>,
  fulfiller: TaskFulfiller<Result<T>>,
}
impl<T> AsyncIOTask<T> {
  pub const fn new(
    task_type: TaskType,
    toward: Arc<File>,
    fulfiller: TaskFulfiller<Result<T>>,
  ) -> Self {
    Self {
      task_type,
      toward,
      fulfiller,
    }
  }
  pub const fn get_type(&self) -> TaskType {
    self.task_type
  }

  pub fn toward(&self) -> &'_ File {
    &self.toward
  }

  pub fn fulfill(self, result: Result<T>) {
    self.fulfiller.fulfill(result);
  }
}
