use std::{io::Result, num::NonZero, thread::available_parallelism};

use crate::background::{SharedWorkThread, ThreadBuilder};

use super::super::{fallocate, pwrite, pwritev, AsyncIOTask};
use super::TaskType;

fn handle_task(task: AsyncIOTask<usize>) {
  let file = task.toward();
  let result = match task.get_type() {
    TaskType::Pwrite { offset, buf } => pwrite(file, buf, offset),
    TaskType::Pwritev { offset, bufs } => pwritev(file, bufs, offset),
    TaskType::Fsync => file.sync_all().map(|_| 0),
    TaskType::Fdatasync => file.sync_data().map(|_| 0),
    TaskType::Fallocate { offset, len } => fallocate(file, offset, len).map(|_| 0),
  };
  task.fulfill(result);
}
pub struct AsyncIO {
  thread: SharedWorkThread<AsyncIOTask<usize>, ()>,
}
impl AsyncIO {
  pub fn new(_: u32) -> Result<Self> {
    let count = available_parallelism().map(NonZero::get).unwrap_or(1);
    let thread = ThreadBuilder::new()
      .name("async io")
      .multi(count)
      .shared(handle_task);
    Ok(Self { thread })
  }

  pub fn submit(&self, task: AsyncIOTask<usize>) {
    self.thread.dispatch(task);
  }

  pub fn close(&self) {
    self.thread.close();
  }
}
