use std::mem::transmute;

use super::{IOHandle, Page, Pointer};
use crate::error::Result;

/**
 * Just a wrapper for IOHandle that provides a logical offset function.
 */
pub struct DiskController<const N: usize> {
  handle: IOHandle,
}
impl<const N: usize> DiskController<N> {
  const SIZE: Pointer = N as Pointer;

  pub const fn new(handle: IOHandle) -> Self {
    Self { handle }
  }

  pub fn read(&self, pointer: Pointer, page: &mut Page<N>) -> Result {
    self.handle.read(pointer * Self::SIZE, page.as_mut())
  }

  pub fn read_unchecked(&self, pointer: Pointer, page: &mut Page<N>) -> Result {
    self
      .handle
      .read_unchecked(pointer * Self::SIZE, page.as_mut())
  }

  #[inline]
  pub fn write(&self, pointer: Pointer, page: &Page<N>) -> Result {
    // transmute allowed since page lifetime available until wait called.
    self
      .handle
      .write_async(pointer * Self::SIZE, unsafe { transmute(page.as_ref()) })
      .wait()
  }

  #[inline]
  pub fn fsync(&self) -> Result {
    self.handle.fsync()
  }

  #[inline]
  pub fn len(&self) -> Result<Pointer> {
    Ok(self.handle.len()? / Self::SIZE)
  }

  pub fn truncate(&self) -> Result {
    self.handle.truncate()
  }
}
