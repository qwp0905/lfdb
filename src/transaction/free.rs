use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam::queue::SegQueue;

use crate::disk::Pointer;

/**
 * Free page list, reconstructed at startup via a full B-tree scan.
 */
pub struct FreeList {
  file_end: AtomicU64,
  released: SegQueue<Pointer>,
}
impl FreeList {
  pub fn new(file_end: Pointer) -> Self {
    Self {
      file_end: AtomicU64::new((file_end == 0).then(|| 1).unwrap_or(file_end)),
      released: SegQueue::new(),
    }
  }

  pub fn alloc(&self) -> Pointer {
    self
      .released
      .pop()
      .unwrap_or_else(|| self.file_end.fetch_add(1, Ordering::Release))
  }

  pub fn dealloc(&self, pointer: Pointer) {
    self.released.push(pointer);
  }
}
