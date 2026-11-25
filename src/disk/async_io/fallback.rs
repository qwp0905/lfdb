use std::ffi::OsStr;
use std::path::Path;
use std::{io::Result, num::NonZero, thread::available_parallelism};

use crate::background::{SharedWorkThread, ThreadBuilder};

use super::super::{fallocate, into_raw, pwrite, pwritev, BoundIOTask};
use super::TaskType;

fn handle_task(task: BoundIOTask) {
  let result = match task.get_type() {
    TaskType::Pwrite { offset, buf } => pwrite(task.toward().unwrap(), buf, *offset),
    TaskType::Pwritev { offset, bufs } => pwritev(task.toward().unwrap(), bufs, *offset),
    TaskType::Fsync => task.toward().unwrap().sync_all().map(|_| 0),
    TaskType::Fdatasync => task.toward().unwrap().sync_data().map(|_| 0),
    TaskType::Fallocate { offset, len } => {
      fallocate(task.toward().unwrap(), *offset, *len).map(|_| 0)
    }
    // SAFETY: the bytes come from `into_encoded_bytes` on this platform, unchanged
    // except for the trailing nul that `to_bytes` leaves out.
    TaskType::Open { path, flags } => flags
      .open(Path::new(unsafe {
        OsStr::from_encoded_bytes_unchecked(path.to_bytes())
      }))
      .map(into_raw),
  };
  task.fulfill(result);
}
pub struct AsyncIO {
  thread: SharedWorkThread<BoundIOTask>,
}
impl AsyncIO {
  pub fn new(_: u32) -> Result<Self> {
    let count = available_parallelism().map(NonZero::get).unwrap_or(1);
    let thread = ThreadBuilder::new()
      .name("async io")
      .count(count)
      .shared(handle_task);
    Ok(Self { thread })
  }

  pub fn submit(&self, task: BoundIOTask) {
    self.thread.dispatch(task);
  }

  pub fn close(&self) {
    self.thread.close();
  }
}
