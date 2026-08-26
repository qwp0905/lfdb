/**
 * Aligned byte buffers for direct I/O.
 *
 * These types are alignment utilities, not storage pages. `AlignedArray` is the
 * minimum fixed-size aligned buffer used for one alignment unit, while
 * `AlignedBuf` is a dynamically sized heap buffer whose allocation size is
 * rounded up to the required alignment.
 */
use std::{
  alloc::{alloc_zeroed, dealloc, handle_alloc_error, Layout},
  ops::{Deref, DerefMut},
  slice::{from_raw_parts, from_raw_parts_mut},
};

use super::ALIGN;

/**
 * Fixed one-alignment-unit buffer.
 *
 * This is useful as a small staging buffer when direct I/O requires both the
 * address and the length to be aligned.
 */
#[repr(align(512))]
pub struct AlignedArray([u8; ALIGN]);
impl AlignedArray {
  pub const fn new() -> Self {
    Self([0; ALIGN])
  }
}
impl Deref for AlignedArray {
  type Target = [u8; ALIGN];

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}
impl DerefMut for AlignedArray {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

const ALIGN_MASK: usize = ALIGN - 1;
const ALIGN_BITS: u32 = ALIGN.trailing_zeros();
/**
 * Round the requested logical length up to the next ALIGN-sized physical
 * buffer length. ALIGN is a power of two, so this is ceil(len / ALIGN) * ALIGN.
 */
const fn aligned_len(len: usize) -> usize {
  ((len + ALIGN_MASK) >> ALIGN_BITS) << ALIGN_BITS
}

/**
 * Dynamically sized aligned heap buffer.
 *
 * `len` is the logical byte length requested by the caller. The actual
 * allocation size is rounded up to an `ALIGN` multiple so the buffer can be
 * used for direct I/O.
 */
pub struct AlignedBuf {
  ptr: *mut u8,
  len: usize,
  layout: Layout,
}
impl AlignedBuf {
  pub fn new(len: usize) -> Self {
    let layout = unsafe { Layout::from_size_align_unchecked(aligned_len(len), ALIGN) };
    Self::from_layout(layout, len)
  }
  fn from_layout(layout: Layout, len: usize) -> Self {
    let ptr = unsafe { alloc_zeroed(layout) };
    if ptr.is_null() {
      handle_alloc_error(layout);
    }
    Self { ptr, len, layout }
  }
  pub const fn size(&self) -> usize {
    self.layout.size()
  }
  pub fn from_vec(data: Vec<u8>) -> Self {
    let mut buf = Self::new(data.len());
    buf.as_mut_slice().copy_from_slice(&data);
    buf
  }
  /**
   * Return the caller-visible logical bytes.
   *
   * These methods mirror ordinary slice/vector APIs and expose only the requested
   * length, not the padded allocation.
   */
  pub const fn as_slice(&self) -> &[u8] {
    unsafe { from_raw_parts(self.ptr, self.len) }
  }
  pub const fn as_mut_slice(&mut self) -> &mut [u8] {
    unsafe { from_raw_parts_mut(self.ptr, self.len) }
  }
  /**
   * Return the full aligned allocation.
   *
   * Direct I/O often requires the submitted length to be aligned as well as the
   * address. These methods expose the padded physical buffer, including any
   * zero-filled bytes beyond the logical length.
   */
  pub const fn get_aligned_slice(&self) -> &[u8] {
    unsafe { from_raw_parts(self.ptr, self.size()) }
  }
  pub const fn get_mut_aligned_slice(&mut self) -> &mut [u8] {
    unsafe { from_raw_parts_mut(self.ptr, self.size()) }
  }
  pub const fn len(&self) -> usize {
    self.len
  }
}
impl Drop for AlignedBuf {
  fn drop(&mut self) {
    unsafe { dealloc(self.ptr, self.layout) };
  }
}
unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}
