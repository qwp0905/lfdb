use std::{cell::UnsafeCell, mem::MaybeUninit};

use super::CachedBlock;

/**
 * Lazily initialized cache-slot storage.
 *
 * The cache allocates the slot array up front, but a `CachedBlock` is only
 * written into a slot once the mapping table assigns that slot. Drop therefore
 * only visits the initialized ranges reported by the mapping table.
 */
pub struct BlockCell(UnsafeCell<MaybeUninit<CachedBlock>>);
impl BlockCell {
  pub const fn uninit() -> Self {
    Self(UnsafeCell::new(MaybeUninit::uninit()))
  }
  pub const fn get(&self) -> &CachedBlock {
    unsafe { (*self.0.get()).assume_init_ref() }
  }
  pub const fn write(&self, block: CachedBlock) {
    unsafe { (*self.0.get()).write(block) };
  }
  pub const fn replace(&self, block: CachedBlock) -> CachedBlock {
    unsafe { (*self.0.get()).as_mut_ptr().replace(block) }
  }
  pub fn drop_in_place(&self) {
    unsafe { (*self.0.get()).assume_init_drop() };
  }
}
unsafe impl Send for BlockCell {}
unsafe impl Sync for BlockCell {}
