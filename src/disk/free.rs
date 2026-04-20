use std::sync::{
  atomic::{AtomicU64, Ordering},
  Arc,
};

use crossbeam::{epoch, queue::SegQueue};

use crate::utils::ToArc;

use super::Pointer;

/**
 * Free page list, reconstructed at startup via a full B-tree scan.
 */
pub struct FreeList {
  file_end: AtomicU64,
  released: Arc<SegQueue<Pointer>>,
}
impl FreeList {
  pub fn new() -> Self {
    Self {
      file_end: AtomicU64::new(1),
      released: SegQueue::new().to_arc(),
    }
  }

  pub fn alloc(&self) -> Pointer {
    self
      .released
      .pop()
      .unwrap_or_else(|| self.file_end.fetch_add(1, Ordering::Release))
  }

  pub fn dealloc(&self, pointer: Pointer) {
    let released = self.released.clone();
    epoch::pin().defer(move || released.push(pointer));
  }
  pub fn replay(&self, file_end: Pointer) {
    self.file_end.store(file_end, Ordering::Release);
  }

  #[inline]
  pub fn file_len(&self) -> Pointer {
    self.file_end.load(Ordering::Acquire)
  }

  #[inline]
  pub fn dead_count(&self) -> usize {
    self.released.len()
  }
}
