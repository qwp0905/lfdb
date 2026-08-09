use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam::queue::SegQueue;

use super::Pointer;

pub enum FreePointer {
  Reuse(Pointer),
  Alloc(Pointer),
}

/**
 * Free page list, reconstructed at startup via a full B-tree scan.
 */
pub struct FreeList {
  file_end: AtomicU64,
  released: SegQueue<Pointer>,
}
impl FreeList {
  pub const fn new() -> Self {
    Self {
      file_end: AtomicU64::new(1),
      released: SegQueue::new(),
    }
  }

  pub fn alloc(&self) -> FreePointer {
    if let Some(ptr) = self.released.pop() {
      return FreePointer::Reuse(ptr);
    }
    FreePointer::Alloc(self.file_end.fetch_add(1, Ordering::Relaxed))
  }

  pub fn dealloc(&self, pointer: Pointer) {
    self.released.push(pointer);
  }
  pub fn replay(&self, file_end: Pointer) {
    self.file_end.store(file_end, Ordering::Relaxed);
  }

  #[inline]
  pub fn file_len(&self) -> Pointer {
    self.file_end.load(Ordering::Relaxed)
  }
}
