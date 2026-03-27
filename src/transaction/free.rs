use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam::queue::SegQueue;

/**
 * Free page list, reconstructed at startup via a full B-tree scan.
 */
pub struct FreeList {
  file_end: AtomicUsize,
  released: SegQueue<usize>,
}
impl FreeList {
  pub fn new(file_end: usize) -> Self {
    Self {
      file_end: AtomicUsize::new((file_end == 0).then(|| 1).unwrap_or(file_end)),
      released: SegQueue::new(),
    }
  }

  pub fn alloc(&self) -> usize {
    self
      .released
      .pop()
      .unwrap_or_else(|| self.file_end.fetch_add(1, Ordering::Release))
  }

  pub fn dealloc(&self, index: usize) {
    self.released.push(index);
  }
}
