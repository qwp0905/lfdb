use std::{
  alloc::{alloc_zeroed, dealloc, handle_alloc_error, Layout},
  marker::PhantomData,
  ops::Range,
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
 * Fixed-size disk block backed by aligned heap memory.
 *
 * `Page` has two responsibilities: it represents one disk block used by the
 * storage layer, and it guarantees the memory alignment required by direct I/O.
 * The buffer is allocated manually with `ALIGN` alignment and freed with the
 * same layout in `Drop`.
 *
 * `ALIGN` is fixed to 512 bytes for direct I/O. `T` is additionally kept as a
 * power-of-two engine block size; this is an engine storage policy, not a
 * requirement of `Layout` itself.
 */
#[derive(Debug)]
pub struct Page<const T: usize = PAGE_SIZE>(*mut u8, PhantomData<[u8; T]>);

impl<const T: usize> Page<T> {
  const LAYOUT: Layout = {
    assert!(T.is_power_of_two());
    unsafe { Layout::from_size_align_unchecked(T, ALIGN) }
  };

  #[inline]
  pub fn new() -> Self {
    let ptr = unsafe { alloc_zeroed(Self::LAYOUT) };
    if ptr.is_null() {
      handle_alloc_error(Self::LAYOUT);
    }
    Self(ptr, PhantomData)
  }
  #[inline]
  pub const fn copy_from(&mut self, data: &[u8], offset: usize) {
    let len = data.len();
    self.range_mut(offset..(offset + len)).copy_from_slice(data);
  }
  #[inline]
  pub fn copy_range(&self, range: Range<usize>) -> Vec<u8> {
    self.range(range).to_vec()
  }
  pub const fn as_slice(&self) -> &[u8] {
    unsafe { from_raw_parts(self.0, T) }
  }
  pub const fn as_mut_slice(&mut self) -> &mut [u8] {
    unsafe { from_raw_parts_mut(self.0, T) }
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
  pub const fn range_mut(&mut self, range: Range<usize>) -> &mut [u8] {
    unsafe { from_raw_parts_mut(self.0.add(range.start), range.end - range.start) }
  }
}

impl<const T: usize> Drop for Page<T> {
  #[inline(always)]
  fn drop(&mut self) {
    unsafe { dealloc(self.0, Self::LAYOUT) };
  }
}

// Page is an owned byte buffer. Moving it across threads is safe, and shared
// references only expose immutable byte access; mutation still requires `&mut`.
unsafe impl<const T: usize> Send for Page<T> {}
unsafe impl<const T: usize> Sync for Page<T> {}

const EOF: Error = Error::EOF;

/**
 * Sequential reader over a `Page`.
 *
 * This wraps `OffsetReader` with the page size and converts out-of-bounds reads
 * into the engine's `Error::EOF`.
 */
pub struct PageScanner<'a, const T: usize = PAGE_SIZE>(OffsetReader<'a>);
impl<'a, const T: usize> PageScanner<'a, T> {
  const fn new(inner: *mut u8) -> Self {
    Self(unsafe { OffsetReader::from_ptr(inner, T) })
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
  pub const fn read_u32(&mut self) -> Result<u32> {
    match self.0.read_u32() {
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

/**
 * Sequential writer over a `Page`.
 *
 * This wraps `OffsetWriter` with the page size and converts out-of-bounds
 * writes into the engine's `Error::EOF`.
 */
pub struct PageWriter<'a, const T: usize = PAGE_SIZE>(OffsetWriter<'a>);
impl<'a, const T: usize> PageWriter<'a, T> {
  const fn new(inner: *mut u8) -> Self {
    Self(unsafe { OffsetWriter::from_ptr(inner, T) })
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
  pub const fn write_u32(&mut self, value: u32) -> Result {
    match self.0.write_u32(value) {
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
