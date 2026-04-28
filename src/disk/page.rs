use std::{
  alloc::{alloc_zeroed, dealloc, Layout},
  marker::PhantomData,
  mem::replace,
  ops::Range,
  panic::RefUnwindSafe,
  ptr::copy_nonoverlapping,
  slice::{from_raw_parts, from_raw_parts_mut},
};

use crate::error::{Error, Result};

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
  pub const fn as_ptr(&self) -> *const u8 {
    self.0 as *const u8
  }
  #[inline]
  pub fn copy_from<V: AsRef<[u8]>>(&mut self, data: V) {
    let data = data.as_ref();
    let len = data.len().min(T);
    unsafe { copy_nonoverlapping(data.as_ptr(), self.0, len) };
  }
  #[inline]
  pub fn copy_n(&mut self, byte_len: usize) -> Vec<u8> {
    let mut data = vec![0; byte_len];
    unsafe { copy_nonoverlapping(self.0, data.as_mut_ptr(), byte_len) };
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
    unsafe { copy_nonoverlapping(value.as_ptr(), page.as_ptr() as *mut u8, len) };
    page
  }
}

// Page itself is a plain byte buffer with no internal synchronization.
unsafe impl<const T: usize> Send for Page<T> {}
unsafe impl<const T: usize> Sync for Page<T> {}
impl<const T: usize> RefUnwindSafe for Page<T> {}

/**
 * A cursor for reading a Page via raw pointer arithmetic.
 *
 * PhantomData ties the scanner's lifetime to the Page it points into.
 * Without it, the compiler would not know that the raw pointer depends
 * on the Page's lifetime, allowing use-after-free.
 */
pub struct PageScanner<'a, const T: usize = PAGE_SIZE> {
  inner: *const u8,
  offset: usize,
  _marker: PhantomData<&'a Page<T>>,
}
impl<'a, const T: usize> PageScanner<'a, T> {
  const fn new(inner: *const u8) -> Self {
    Self {
      inner,
      offset: 0,
      _marker: PhantomData,
    }
  }

  pub const fn advance(&mut self, n: usize) -> Result<usize> {
    let end = self.offset + n;
    if end > T {
      return Err(Error::EOF);
    }
    Ok(replace(&mut self.offset, end))
  }

  pub const fn read(&mut self) -> Result<u8> {
    if self.offset >= T {
      return Err(Error::EOF);
    }
    let v = unsafe { *self.inner.add(self.offset) };
    self.offset += 1;
    Ok(v)
  }

  pub const fn read_n(&mut self, n: usize) -> Result<&'a [u8]> {
    let end = self.offset + n;
    if end > T {
      return Err(Error::EOF);
    }
    let b = unsafe { from_raw_parts(self.inner.add(self.offset), n) };
    self.offset = end;
    Ok(b)
  }

  #[inline(always)]
  pub const fn read_u64(&mut self) -> Result<u64> {
    if self.offset + 8 > T {
      return Err(Error::EOF);
    }
    let v = unsafe { (self.inner.add(self.offset) as *const [u8; 8]).read() };
    self.offset += 8;
    Ok(u64::from_le_bytes(v))
  }
  #[inline(always)]
  pub const fn read_u16(&mut self) -> Result<u16> {
    if self.offset + 2 > T {
      return Err(Error::EOF);
    }
    let v = unsafe { (self.inner.add(self.offset) as *const [u8; 2]).read() };
    self.offset += 2;
    Ok(u16::from_le_bytes(v))
  }
}

/**
 * A cursor for writing into a Page via raw pointer arithmetic.
 *
 * PhantomData ties the writer's lifetime to the Page it points into.
 * Without it, the compiler would not know that the raw pointer depends
 * on the Page's lifetime, allowing use-after-free.
 */
pub struct PageWriter<'a, const T: usize = PAGE_SIZE> {
  inner: *mut u8,
  offset: usize,
  marker: PhantomData<&'a Page<T>>,
}
impl<'a, const T: usize> PageWriter<'a, T> {
  const fn new(inner: *mut u8) -> Self {
    Self {
      inner,
      offset: 0,
      marker: PhantomData,
    }
  }

  pub const fn write(&mut self, bytes: &[u8]) -> Result<()> {
    let len = bytes.len();
    let end = self.offset + len;
    if end > T {
      return Err(Error::EOF);
    };
    unsafe { copy_nonoverlapping(bytes.as_ptr(), self.inner.add(self.offset), len) };
    self.offset = end;
    Ok(())
  }

  #[inline(always)]
  pub const fn write_u64(&mut self, value: u64) -> Result {
    self.write(&value.to_le_bytes())
  }
  #[inline(always)]
  pub const fn write_u16(&mut self, value: u16) -> Result {
    self.write(&value.to_le_bytes())
  }
  #[inline(always)]
  pub const fn write_u8(&mut self, value: u8) -> Result {
    self.write(&value.to_le_bytes())
  }

  #[inline(always)]
  pub const fn finalize(self) -> usize {
    self.offset
  }
}

#[cfg(test)]
#[path = "tests/page.rs"]
mod tests;
