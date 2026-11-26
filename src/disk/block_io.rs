use std::io;

use super::{IOHandle, PendingIO, Pointer};
use crate::{
  error::Result,
  page::{Page, PageMut},
  Error,
};

/**
 * Block-addressed wrapper around `IOHandle`.
 *
 * `IOHandle` works with byte offsets and `std::io::Error`. This wrapper maps a
 * logical block pointer to a byte offset (`pointer * N`) and converts IO errors
 * into the engine error type. Writes take a `'static` page view; the caller that
 * owns the buffer keeps it alive in the completion callback.
 */
pub struct BlockIOHandle<const N: usize> {
  handle: IOHandle,
}
impl<const N: usize> BlockIOHandle<N> {
  const SHIFT: u32 = {
    assert!(N.is_power_of_two());
    N.ilog2()
  };
  #[inline(always)]
  const fn cvt(pointer: Pointer) -> u64 {
    pointer << Self::SHIFT
  }

  pub const fn new(handle: IOHandle) -> Self {
    Self { handle }
  }

  pub fn read(&self, pointer: Pointer, page: PageMut<N>) -> Result {
    self
      .handle
      .read(page.into_mut_slice(), Self::cvt(pointer))
      .map_err(Error::IO)
  }

  pub fn write_async_with<
    T: Send + 'static,
    F: FnOnce(io::Result<()>) -> T + Send + 'static,
  >(
    &self,
    pointer: Pointer,
    page: Page<'static, N>,
    callback: F,
  ) -> PendingIO<T> {
    let slice = page.as_slice();
    self
      .handle
      .write_async_with(slice, Self::cvt(pointer), callback)
  }

  #[inline]
  pub fn fsync(&self) -> PendingIO {
    self.handle.fsync_async()
  }

  pub fn truncate(&self) -> Result {
    self.handle.truncate().map_err(Error::IO)
  }
}
