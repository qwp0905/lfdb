use std::ffi::OsStr;
use std::path::Path;
use std::{io::Result, num::NonZero, thread::available_parallelism};

use crate::background::{SharedWorkThread, ThreadBuilder};

use super::super::{fallocate, into_raw, pwrite, pwritev, BoundIOTask};

fn handle_task(task: BoundIOTask) {
  match task {
    BoundIOTask::Pwrite {
      offset,
      buf,
      fulfiller,
      file,
    } => fulfiller.fulfill(pwrite(&file, buf, offset)),
    BoundIOTask::Pwritev {
      offset,
      bufs,
      fulfiller,
      file,
    } => fulfiller.fulfill(pwritev(&file, bufs, offset)),
    BoundIOTask::Fsync { file, fulfiller } => fulfiller.fulfill(file.sync_all()),
    BoundIOTask::Fdatasync { file, fulfiller } => fulfiller.fulfill(file.sync_data()),
    BoundIOTask::Fallocate {
      offset,
      len,
      file,
      fulfiller,
    } => fulfiller.fulfill(fallocate(&file, offset, len)),
    // SAFETY: the bytes come from `into_encoded_bytes` on this platform, unchanged
    // except for the trailing nul that `to_bytes` leaves out.
    BoundIOTask::Open {
      path,
      flags,
      fulfiller,
    } => fulfiller.fulfill(
      flags
        .open(Path::new(unsafe {
          OsStr::from_encoded_bytes_unchecked(path.to_bytes())
        }))
        .map(into_raw),
    ),
  };
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
