use std::{io::Result, num::NonZero, thread::available_parallelism};

use crate::background::{SharedWorkThread, ThreadBuilder};

use super::super::{fallocate, into_raw, pwrite, pwritev, IOTask};
use super::TaskType;

fn handle_task(task: IOTask) {
  let result = match task.context().get_type() {
    TaskType::Pwrite { offset, buf } => pwrite(task.context().toward(), buf, *offset),
    TaskType::Pwritev { offset, bufs } => pwritev(task.context().toward(), bufs, *offset),
    TaskType::Fsync => task.context().toward().sync_all().map(|_| 0),
    TaskType::Fdatasync => task.context().toward().sync_data().map(|_| 0),
    TaskType::Fallocate { offset, len } => {
      fallocate(task.context().toward(), *offset, *len).map(|_| 0)
    }
    TaskType::Open { path, flags } => flags.open(path).map(into_raw),
  };
  task.fulfill(result);
}
pub struct AsyncIO {
  thread: SharedWorkThread<IOTask>,
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

  pub fn submit(&self, task: IOTask) {
    self.thread.dispatch(task);
  }

  pub fn close(&self) {
    self.thread.close();
  }
}
