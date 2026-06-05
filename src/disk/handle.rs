use std::mem::transmute;

use super::{IOHandle, Page, Pointer};
use crate::{error::Result, thread::TaskHandle};

/**
 * Just a wrapper for IOHandle that provides a logical offset function.
 */
pub struct BlockHandle<const N: usize> {
  handle: IOHandle,
}
impl<const N: usize> BlockHandle<N> {
  const SIZE: Pointer = N as Pointer;

  pub const fn new(handle: IOHandle) -> Self {
    Self { handle }
  }

  pub fn read(&self, pointer: Pointer, page: &mut Page<N>) -> Result {
    self.handle.read(page.as_mut(), pointer * Self::SIZE)
  }

  pub fn read_unchecked(&self, pointer: Pointer, page: &mut Page<N>) -> Result {
    self
      .handle
      .read_unchecked(page.as_mut(), pointer * Self::SIZE)
  }

  #[inline]
  pub fn write(&self, pointer: Pointer, page: &Page<N>) -> Result {
    // transmute allowed since page lifetime available until wait called.
    self.write_async(pointer, unsafe { transmute(page) }).wait()
  }

  pub fn write_async(&self, pointer: Pointer, page: &'static Page<N>) -> TaskHandle<()> {
    self.handle.write_async(page.as_ref(), pointer * Self::SIZE)
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
