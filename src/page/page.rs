use std::{
  marker::PhantomData,
  mem::transmute,
  ops::Range,
  slice::{from_raw_parts, from_raw_parts_mut},
};

use crate::{
  error::Result,
  utils::{OffsetReader, OffsetWriter},
  Error,
};

pub const PAGE_SIZE: usize = 4 << 10; // 4 kb

/**
 * Read-only view of one fixed-size disk block.
 *
 * The buffer is owned elsewhere, and `'a` ties the view to that owner. Any number
 * of views may exist at once, so a view never grants write access.
 *
 * Buffers are aligned to `ALIGN`, fixed to 512 bytes for direct I/O. `N` is kept
 * as a power-of-two engine block size; this is an engine storage policy, not a
 * requirement of `Layout` itself.
 */
#[derive(Debug, Clone, Copy)]
pub struct Page<'a, const N: usize = PAGE_SIZE>(*const u8, PhantomData<&'a [u8; N]>);
impl<'a, const N: usize> Page<'a, N> {
  pub const unsafe fn new(ptr: *const u8) -> Self {
    Self(ptr, PhantomData)
  }
  pub const fn as_ptr(&self) -> *const u8 {
    self.0
  }
  /**
   * # Safety
   * No other access may overlap the written range while it is written.
   */
  pub const unsafe fn copy_from_unchecked(&self, data: &[u8], offset: usize) {
    let ptr = self.0.add(offset).cast_mut();
    data.as_ptr().copy_to_nonoverlapping(ptr, data.len());
  }
  pub fn copy_range(&self, range: Range<usize>) -> Vec<u8> {
    self.range(range).to_vec()
  }
  pub const fn as_slice(&self) -> &'a [u8] {
    unsafe { from_raw_parts(self.0, N) }
  }
  pub const fn scanner(&self) -> PageScanner<'a, N> {
    PageScanner::new(self.0)
  }
  pub const fn range(&self, range: Range<usize>) -> &'a [u8] {
    unsafe { from_raw_parts(self.0.add(range.start), range.end - range.start) }
  }

  /**
   * # Safety
   * The owner of the buffer must outlive every use of the returned view.
   */
  pub const unsafe fn into_static(self) -> Page<'static, N> {
    unsafe { transmute::<Self, Page<'static, N>>(self) }
  }
}

/**
 * Unique write view of one block. Like `&mut [u8]` it is never copied;
 * `reborrow` lends it for a shorter lifetime.
 */
#[derive(Debug)]
pub struct PageMut<'a, const N: usize>(*mut u8, PhantomData<&'a mut [u8; N]>);
impl<'a, const N: usize> PageMut<'a, N> {
  pub const unsafe fn new(ptr: *mut u8) -> Self {
    Self(ptr, PhantomData)
  }
  pub const fn as_ptr(&self) -> *mut u8 {
    self.0
  }
  pub const fn copy_from(&'a mut self, data: &[u8], offset: usize) {
    let len = data.len();
    self.range_mut(offset..(offset + len)).copy_from_slice(data);
  }
  pub const fn into_mut_slice(self) -> &'a mut [u8] {
    unsafe { from_raw_parts_mut(self.0, N) }
  }
  pub const fn range(&'a self, range: Range<usize>) -> &'a [u8] {
    unsafe { from_raw_parts(self.0.add(range.start), range.end - range.start) }
  }
  pub const fn range_mut(&'a mut self, range: Range<usize>) -> &'a mut [u8] {
    unsafe { from_raw_parts_mut(self.0.add(range.start), range.end - range.start) }
  }
  pub const fn writer(self) -> PageWriter<'a, N> {
    PageWriter::new(self.0)
  }
  pub const fn reborrow(&mut self) -> PageMut<'_, N> {
    unsafe { PageMut::new(self.0) }
  }
}

/**
 * Both views behave like the references they stand for: `Page` like `&[u8]` and
 * `PageMut` like `&mut [u8]`, so they cross threads under the same rules.
 */
unsafe impl<'a, const N: usize> Send for Page<'a, N> {}
unsafe impl<'a, const N: usize> Sync for Page<'a, N> {}
unsafe impl<'a, const N: usize> Send for PageMut<'a, N> {}
unsafe impl<'a, const N: usize> Sync for PageMut<'a, N> {}

/**
 * Sequential reader over a `Page`.
 *
 * This wraps `OffsetReader` with the page size and converts out-of-bounds reads
 * into the engine's `Error::Corrupted`.
 */
pub struct PageScanner<'a, const N: usize = PAGE_SIZE>(OffsetReader<'a>);
impl<'a, const N: usize> PageScanner<'a, N> {
  const OUT_OF_BOUNDS: Error = Error::Corrupted("page read out of bounds");

  const fn new(inner: *const u8) -> Self {
    Self(unsafe { OffsetReader::from_ptr(inner, N) })
  }

  pub const fn advance(&mut self, n: usize) -> Result<usize> {
    match self.0.advance(n) {
      Some(offset) => Ok(offset),
      None => Err(Self::OUT_OF_BOUNDS),
    }
  }

  pub const fn read(&mut self) -> Result<u8> {
    match self.0.read_byte() {
      Some(v) => Ok(v),
      None => Err(Self::OUT_OF_BOUNDS),
    }
  }

  pub const fn read_n(&mut self, n: usize) -> Result<&'a [u8]> {
    match self.0.read(n) {
      Some(v) => Ok(v),
      None => Err(Self::OUT_OF_BOUNDS),
    }
  }

  pub const fn read_u64(&mut self) -> Result<u64> {
    match self.0.read_u64() {
      Some(v) => Ok(v),
      None => Err(Self::OUT_OF_BOUNDS),
    }
  }

  pub const fn read_u32(&mut self) -> Result<u32> {
    match self.0.read_u32() {
      Some(v) => Ok(v),
      None => Err(Self::OUT_OF_BOUNDS),
    }
  }

  pub const fn read_u16(&mut self) -> Result<u16> {
    match self.0.read_u16() {
      Some(v) => Ok(v),
      None => Err(Self::OUT_OF_BOUNDS),
    }
  }
}

/**
 * Sequential writer over a `Page`.
 *
 * This wraps `OffsetWriter` with the page size and converts out-of-bounds
 * writes into the engine's `Error::Corrupted`.
 */
pub struct PageWriter<'a, const N: usize = PAGE_SIZE>(OffsetWriter<'a>);
impl<'a, const N: usize> PageWriter<'a, N> {
  const OVERFLOW: Error = Error::Corrupted("page write overflow");

  const fn new(inner: *mut u8) -> Self {
    Self(unsafe { OffsetWriter::from_ptr(inner, N) })
  }

  pub const fn write(&mut self, bytes: &[u8]) -> Result<()> {
    match self.0.write(bytes) {
      true => Ok(()),
      false => Err(Self::OVERFLOW),
    }
  }

  pub const fn write_u64(&mut self, value: u64) -> Result {
    match self.0.write_u64(value) {
      true => Ok(()),
      false => Err(Self::OVERFLOW),
    }
  }

  pub const fn write_u32(&mut self, value: u32) -> Result {
    match self.0.write_u32(value) {
      true => Ok(()),
      false => Err(Self::OVERFLOW),
    }
  }

  pub const fn write_u16(&mut self, value: u16) -> Result {
    match self.0.write_u16(value) {
      true => Ok(()),
      false => Err(Self::OVERFLOW),
    }
  }

  pub const fn finalize(self) -> usize {
    self.0.written_bytes()
  }
}

#[cfg(all(test, not(loom)))]
#[path = "tests/page.rs"]
mod tests;
