use std::{
  alloc::{alloc, dealloc, Layout},
  ops::{Deref, DerefMut},
  ptr::copy_nonoverlapping,
  slice::{from_raw_parts, from_raw_parts_mut},
};

use super::ALIGN;

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
const fn aligned_len(len: usize) -> usize {
  ((len + ALIGN_MASK) >> ALIGN_BITS) << ALIGN_BITS
}
pub struct AlignedBuf {
  ptr: *mut u8,
  len: usize,
  layout: Layout,
}
impl AlignedBuf {
  pub fn new(len: usize) -> Self {
    let layout = unsafe { Layout::from_size_align_unchecked(aligned_len(len), ALIGN) };
    let ptr = unsafe { alloc(layout) };
    Self { ptr, len, layout }
  }
  pub const fn size(&self) -> usize {
    self.layout.size()
  }
  pub fn from_vec(data: Vec<u8>) -> Self {
    let buf = Self::new(data.len());
    unsafe { copy_nonoverlapping(data.as_ptr(), buf.ptr, buf.len) };
    buf
  }
  pub const fn as_slice(&self) -> &[u8] {
    unsafe { from_raw_parts(self.ptr, self.len) }
  }
  pub const fn as_mut_slice(&mut self) -> &mut [u8] {
    unsafe { from_raw_parts_mut(self.ptr, self.len) }
  }
  pub const fn get_aligned_slice(&self) -> &[u8] {
    unsafe { from_raw_parts(self.ptr, self.size()) }
  }
  pub const fn get_mut_aligned_slice(&mut self) -> &mut [u8] {
    unsafe { from_raw_parts_mut(self.ptr, self.size()) }
  }
  pub const fn len(&self) -> usize {
    self.len
  }
  pub const fn as_ptr(&self) -> *const u8 {
    self.ptr
  }
}
impl Drop for AlignedBuf {
  fn drop(&mut self) {
    unsafe { dealloc(self.ptr, self.layout) };
  }
}
unsafe impl Send for AlignedBuf {}
impl Clone for AlignedBuf {
  fn clone(&self) -> Self {
    Self {
      ptr: unsafe { alloc(self.layout) },
      len: self.len,
      layout: self.layout,
    }
  }
}
