use std::fs::File;
use std::io::{IoSlice, Result};
use std::sync::Arc;

use crate::background::OneshotFulfill;

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

pub struct Task {
  pub toward: Arc<File>,
  pub task_type: TaskType,
  pub done: OneshotFulfill<Result<usize>>,
}
