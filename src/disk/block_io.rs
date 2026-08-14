use std::mem::transmute;

use super::{IOHandle, Page, PendingIO, Pointer};
use crate::{error::Result, Error};

/**
 * Block-addressed wrapper around `IOHandle`.
 *
 * `IOHandle` works with byte offsets and `std::io::Error`. This wrapper maps a
 * logical block pointer to a byte offset (`pointer * N`) and converts IO errors
 * into the engine error type. Its synchronous `write` method also closes the
 * lifetime gap around the async write path by waiting before the borrowed page
 * can go out of scope.
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

  pub fn read(&self, pointer: Pointer, page: &mut Page<N>) -> Result {
    self
      .handle
      .read(page.as_mut_slice(), Self::cvt(pointer))
      .map_err(Error::IO)
  }

  pub fn read_unchecked(&self, pointer: Pointer, page: &mut Page<N>) -> Result {
    self
      .handle
      .read_unchecked(page.as_mut_slice(), Self::cvt(pointer))
      .map_err(Error::IO)
  }

  #[inline]
  pub fn write(&self, pointer: Pointer, page: &Page<N>) -> Result {
    // SAFETY: `write_async` requires a `'static` page because the buffer crosses
    // into the IO worker queue. This synchronous wrapper immediately waits for
    // completion, and completion means the worker no longer holds or reads the
    // submitted slice. Therefore the borrowed page cannot outlive this call.
    let static_ref = unsafe { transmute::<&Page<N>, &'static Page<N>>(page) };
    self.write_async(pointer, static_ref).wait()
  }

  /**
   * Submit an asynchronous block write.
   *
   * The page must remain valid until the returned `AsyncIO` completes. Callers
   * that cannot provide that lifetime should use `write`, which waits before
   * returning.
   */
  pub fn write_async(&self, pointer: Pointer, page: &'static Page<N>) -> PendingIO {
    let slice = page.as_slice();
    PendingIO::new(self.handle.alloc_and_write(slice, Self::cvt(pointer)))
  }

  #[inline]
  pub fn fsync(&self) -> Result {
    self.handle.fsync().map_err(Error::IO)
  }

  pub fn truncate(&self) -> Result {
    self.handle.truncate().map_err(Error::IO)
  }
}
