use std::sync::{
  atomic::{AtomicU64, Ordering},
  Mutex,
};

use crossbeam::queue::SegQueue;

use crate::utils::{ChunkQueue, ShortenedMutex};

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
  protection: Mutex<Protection>,
}
impl FreeList {
  pub const fn new() -> Self {
    Self {
      file_end: AtomicU64::new(1),
      released: SegQueue::new(),
      protection: Mutex::new(Protection::new()),
    }
  }

  pub fn alloc(&self) -> FreePointer {
    if let Some(ptr) = self.released.pop() {
      return FreePointer::Reuse(ptr);
    }
    FreePointer::Alloc(self.file_end.fetch_add(1, Ordering::Relaxed))
  }

  pub fn dealloc(&self, pointer: Pointer) {
    let mut protection = self.protection.l();
    if !protection.is_protected {
      self.released.push(pointer);
      return;
    }
    protection.buffered.push(pointer);
  }
  pub fn replay(&self, file_end: Pointer) {
    self.file_end.store(file_end, Ordering::Relaxed);
  }

  #[inline]
  pub fn file_len(&self) -> Pointer {
    self.file_end.load(Ordering::Relaxed)
  }

  pub fn protect(&self) -> DeallocGuard<'_> {
    let mut protection = self.protection.l();
    debug_assert!(!protection.is_protected);
    protection.is_protected = true;
    DeallocGuard(self)
  }
}

pub struct DeallocGuard<'a>(&'a FreeList);
impl<'a> Drop for DeallocGuard<'a> {
  fn drop(&mut self) {
    let mut protection = self.0.protection.l();
    debug_assert!(protection.is_protected);
    while let Some(ptr) = protection.buffered.pop() {
      self.0.released.push(ptr);
    }
    protection.is_protected = false;
  }
}

struct Protection {
  buffered: ChunkQueue<Pointer>,
  is_protected: bool,
}
impl Protection {
  const fn new() -> Self {
    Self {
      buffered: ChunkQueue::new(),
      is_protected: false,
    }
  }
}
