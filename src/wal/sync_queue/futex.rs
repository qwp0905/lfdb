use crossbeam::queue::SegQueue;

use super::*;

use std::{
  ptr::null,
  sync::atomic::{AtomicU32, Ordering},
};

pub struct SyncQueue {
  generation: AtomicU32,
  notification: AtomicU32,
  queue: SegQueue<FsyncResult>,
}
impl SyncQueue {
  pub const fn new() -> Self {
    Self {
      generation: AtomicU32::new(0),
      notification: AtomicU32::new(0),
      queue: SegQueue::new(),
    }
  }

  pub fn push(&self, fsync: FsyncResult) {
    self.queue.push(fsync);
    self.notify(1);
  }

  pub fn wait_until(&self, generation: SegmentGeneration) -> Result<IOResult<()>> {
    while self.generation.load(Ordering::Acquire) < generation {
      if let Some(fsync) = self.queue.pop() {
        let result = fsync.wait()?;
        self.advance();
        if let Err(err) = result {
          return Ok(Err(err));
        }
        continue;
      }

      let observed = self.notification.load(Ordering::Acquire);
      if self.generation.load(Ordering::Acquire) >= generation {
        break;
      }
      if let Some(fsync) = self.queue.pop() {
        let result = fsync.wait()?;
        self.advance();
        if let Err(err) = result {
          return Ok(Err(err));
        }
        continue;
      }

      futex_wait(&self.notification, observed);
    }

    Ok(Ok(()))
  }

  pub fn drain(&self) {
    while let Some(fsync) = self.queue.pop() {
      let _ = fsync.wait();
      self.advance();
    }
  }

  fn advance(&self) {
    self.generation.fetch_add(1, Ordering::Release);
    self.notify(i32::MAX);
  }

  fn notify(&self, count: i32) {
    self.notification.fetch_add(1, Ordering::Release);
    futex_wake(&self.notification, count);
  }
}

fn futex_wait(futex: &AtomicU32, expected: u32) -> bool {
  let result = unsafe {
    libc::syscall(
      libc::SYS_futex,
      futex.as_ptr(),
      libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG,
      expected,
      null::<libc::timespec>(),
      null::<u32>(),
      0_u32,
    )
  };

  result != -1
}

fn futex_wake(futex: &AtomicU32, count: i32) -> bool {
  let result = unsafe {
    libc::syscall(
      libc::SYS_futex,
      futex.as_ptr(),
      libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG,
      count,
      null::<libc::timespec>(),
      null::<u32>(),
      0_u32,
    )
  };

  result != -1
}
