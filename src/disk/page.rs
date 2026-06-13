use std::{
  alloc::{alloc_zeroed, dealloc, Layout},
  marker::PhantomData,
  ops::Range,
  ptr::copy_nonoverlapping,
  slice::{from_raw_parts, from_raw_parts_mut},
};

use crate::{
  error::Result,
  utils::{OffsetReader, OffsetWriter},
  Error,
};

pub const PAGE_SIZE: usize = 4 << 10; // 4 kb

pub const ALIGN: usize = 512;

/**
 * An abstraction over a fixed-size disk block.
 * Allocate 512-byte aligned heap memory for Direct I/O.
 * For memory alignment, the page size must always be a multiple of 2.
 */
#[derive(Debug)]
pub struct Page<const T: usize = PAGE_SIZE>(*mut u8, PhantomData<[u8; T]>);

impl<const T: usize> Page<T> {
  const LAYOUT: Layout = {
    assert!(T & (T - 1) == 0);
    unsafe { Layout::from_size_align_unchecked(T, ALIGN) }
  };

  #[inline]
  pub fn new() -> Self {
    Self(unsafe { alloc_zeroed(Self::LAYOUT) }, PhantomData)
  }
  #[inline(always)]
  pub const fn as_ptr(&self) -> *mut u8 {
    self.0
  }
  #[inline]
  pub fn copy_from<V: AsRef<[u8]>>(&mut self, data: V) {
    let data = data.as_ref();
    let len = data.len().min(T);
    unsafe { copy_nonoverlapping(data.as_ptr(), self.0, len) };
  }
  #[inline]
  pub fn copy_range(&self, range: Range<usize>) -> Vec<u8> {
    let len = range.end - range.start;
    let mut data = Vec::with_capacity(len);
    unsafe { copy_nonoverlapping(self.0.add(range.start), data.as_mut_ptr(), len) };
    unsafe { data.set_len(len) };
    data
  }
  #[inline]
  pub const fn scanner(&self) -> PageScanner<'_, T> {
    PageScanner::new(self.0)
  }
  #[inline]
  pub const fn writer(&mut self) -> PageWriter<'_, T> {
    PageWriter::new(self.0)
  }
  #[inline]
  pub const fn range(&self, range: Range<usize>) -> &[u8] {
    unsafe { from_raw_parts(self.0.add(range.start), range.end - range.start) }
  }
}

impl<const T: usize> Drop for Page<T> {
  #[inline(always)]
  fn drop(&mut self) {
    unsafe { dealloc(self.0, Self::LAYOUT) };
  }
}

impl<const T: usize> AsRef<[u8]> for Page<T> {
  #[inline(always)]
  fn as_ref(&self) -> &[u8] {
    unsafe { from_raw_parts(self.0, T) }
  }
}
impl<const T: usize> AsMut<[u8]> for Page<T> {
  #[inline(always)]
  fn as_mut(&mut self) -> &mut [u8] {
    unsafe { from_raw_parts_mut(self.0, T) }
  }
}

impl<const T: usize> From<&[u8]> for Page<T> {
  #[inline]
  fn from(value: &[u8]) -> Self {
    let page = Self::new();
    let len = value.len().min(T);
    unsafe { copy_nonoverlapping(value.as_ptr(), page.as_ptr(), len) };
    page
  }
}

// Page itself is a plain byte buffer with no internal synchronization.
unsafe impl<const T: usize> Send for Page<T> {}
unsafe impl<const T: usize> Sync for Page<T> {}

const EOF: Error = Error::EOF;

pub struct PageScanner<'a, const T: usize = PAGE_SIZE>(OffsetReader<'a>);
impl<'a, const T: usize> PageScanner<'a, T> {
  const fn new(inner: *const u8) -> Self {
    Self(OffsetReader::from_ptr(inner, T))
  }

  #[inline(always)]
  pub const fn advance(&mut self, n: usize) -> Result<usize> {
    match self.0.advance(n) {
      Some(offset) => Ok(offset),
      None => Err(EOF),
    }
  }

  #[inline(always)]
  pub const fn read(&mut self) -> Result<u8> {
    match self.0.read_byte() {
      Some(v) => Ok(v),
      None => Err(Error::EOF),
    }
  }

  #[inline(always)]
  pub const fn read_n(&mut self, n: usize) -> Result<&'a [u8]> {
    match self.0.read(n) {
      Some(v) => Ok(v),
      None => Err(EOF),
    }
  }

  #[inline(always)]
  pub const fn read_u64(&mut self) -> Result<u64> {
    match self.0.read_u64() {
      Some(v) => Ok(v),
      None => Err(EOF),
    }
  }

  #[inline(always)]
  pub const fn read_u16(&mut self) -> Result<u16> {
    match self.0.read_u16() {
      Some(v) => Ok(v),
      None => Err(EOF),
    }
  }
}

pub struct PageWriter<'a, const T: usize = PAGE_SIZE>(OffsetWriter<'a>);
impl<'a, const T: usize> PageWriter<'a, T> {
  const fn new(inner: *mut u8) -> Self {
    Self(OffsetWriter::from_ptr(inner, T))
  }

  #[inline(always)]
  pub const fn write(&mut self, bytes: &[u8]) -> Result<()> {
    match self.0.write(bytes) {
      true => Ok(()),
      false => Err(EOF),
    }
  }

  #[inline(always)]
  pub const fn write_u64(&mut self, value: u64) -> Result {
    match self.0.write_u64(value) {
      true => Ok(()),
      false => Err(EOF),
    }
  }
  #[inline(always)]
  pub const fn write_u16(&mut self, value: u16) -> Result {
    match self.0.write_u16(value) {
      true => Ok(()),
      false => Err(EOF),
    }
  }
  #[inline(always)]
  pub const fn write_u8(&mut self, value: u8) -> Result {
    match self.0.write_u8(value) {
      true => Ok(()),
      false => Err(EOF),
    }
  }

  #[inline(always)]
  pub const fn finalize(self) -> usize {
    self.0.written_bytes()
  }
}

#[cfg(test)]
#[path = "tests/page.rs"]
mod tests;
