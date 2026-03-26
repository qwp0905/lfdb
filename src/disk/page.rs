use std::{
  cell::UnsafeCell, marker::PhantomData, panic::RefUnwindSafe, ptr::copy_nonoverlapping,
  slice::from_raw_parts,
};

use crate::{
  error::{Error, Result},
  utils::UnsafeBorrow,
};

pub const PAGE_SIZE: usize = 4 << 10; // 4 kb

#[derive(Debug)]
pub struct Page<const T: usize = PAGE_SIZE>(UnsafeCell<[u8; T]>);

impl<const T: usize> Page<T> {
  #[inline]
  pub fn new() -> Self {
    Self(UnsafeCell::new([0; T]))
  }
  #[inline]
  pub fn as_ptr(&self) -> *const u8 {
    self.0.get() as *const u8
  }
  #[inline]
  pub fn copy_from<V: AsRef<[u8]>>(&mut self, data: V) {
    let data = data.as_ref();
    let len = data.len().min(T);
    unsafe { copy_nonoverlapping(data.as_ptr(), self.as_ptr() as *mut u8, len) };
  }
  #[inline]
  pub fn copy_n(&mut self, byte_len: usize) -> Vec<u8> {
    let mut data = vec![0; byte_len];
    unsafe { copy_nonoverlapping(self.as_ptr(), data.as_mut_ptr(), byte_len) };
    data
  }
  #[inline]
  pub fn scanner(&self) -> PageScanner<'_, T> {
    PageScanner::new(self.as_ptr())
  }
  #[inline]
  pub fn writer(&mut self) -> PageWriter<'_, T> {
    PageWriter::new(self.0.get() as *mut u8)
  }
}

impl<const T: usize> AsRef<[u8]> for Page<T> {
  #[inline]
  fn as_ref(&self) -> &[u8] {
    self.0.get().borrow_unsafe()
  }
}
impl<const T: usize> AsMut<[u8]> for Page<T> {
  #[inline]
  fn as_mut(&mut self) -> &mut [u8] {
    self.0.get_mut()
  }
}
impl<const T: usize> From<[u8; T]> for Page<T> {
  #[inline]
  fn from(bytes: [u8; T]) -> Self {
    Self(UnsafeCell::new(bytes))
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
unsafe impl<const T: usize> Send for Page<T> {}
unsafe impl<const T: usize> Sync for Page<T> {}
impl<const T: usize> RefUnwindSafe for Page<T> {}

pub struct PageScanner<'a, const T: usize = PAGE_SIZE> {
  inner: *const u8,
  offset: usize,
  _marker: PhantomData<&'a Page<T>>,
}
impl<'a, const T: usize> PageScanner<'a, T> {
  #[inline]
  fn new(inner: *const u8) -> Self {
    Self {
      inner,
      offset: 0,
      _marker: Default::default(),
    }
  }

  #[inline]
  pub fn read(&mut self) -> Result<u8> {
    if self.offset >= T {
      return Err(Error::EOF);
    }
    let v = unsafe { *self.inner.add(self.offset) };
    self.offset += 1;
    Ok(v)
  }

  #[inline]
  pub fn read_n(&mut self, n: usize) -> Result<&[u8]> {
    let end = self.offset + n;
    if end > T {
      return Err(Error::EOF);
    }
    let b = unsafe { from_raw_parts(self.inner.add(self.offset), n) };
    self.offset = end;
    Ok(b)
  }

  #[inline]
  fn read_const_n<const N: usize>(&mut self) -> Result<[u8; N]> {
    if self.offset + N > T {
      return Err(Error::EOF);
    }
    let v = unsafe { (self.inner.add(self.offset) as *const [u8; N]).read() };
    self.offset += N;
    Ok(v)
  }

  #[inline]
  pub fn read_usize(&mut self) -> Result<usize> {
    self.read_const_n::<8>().map(usize::from_le_bytes)
  }

  #[inline]
  pub fn read_u16(&mut self) -> Result<u16> {
    self.read_const_n::<2>().map(u16::from_le_bytes)
  }

  #[inline]
  pub fn read_u32(&mut self) -> Result<u32> {
    self.read_const_n::<4>().map(u32::from_le_bytes)
  }

  #[inline]
  pub fn is_eof(&self) -> bool {
    T <= self.offset
  }
}

pub struct PageWriter<'a, const T: usize = PAGE_SIZE> {
  inner: *mut u8,
  offset: usize,
  marker: PhantomData<&'a Page<T>>,
}
impl<'a, const T: usize> PageWriter<'a, T> {
  fn new(inner: *mut u8) -> Self {
    Self {
      inner,
      offset: 0,
      marker: Default::default(),
    }
  }

  pub fn write(&mut self, bytes: &[u8]) -> Result<()> {
    let len = bytes.len();
    let end = self.offset + len;
    if end > T {
      return Err(Error::EOF);
    };
    unsafe { copy_nonoverlapping(bytes.as_ptr(), self.inner.add(self.offset), len) };
    self.offset = end;
    Ok(())
  }

  pub fn write_usize(&mut self, value: usize) -> Result<()> {
    self.write(&value.to_le_bytes())
  }
  pub fn write_u32(&mut self, value: u32) -> Result {
    self.write(&value.to_le_bytes())
  }
  pub fn write_u16(&mut self, value: u16) -> Result {
    self.write(&value.to_le_bytes())
  }
  pub fn write_u8(&mut self, value: u8) -> Result {
    self.write(&value.to_le_bytes())
  }

  pub fn finalize(self) -> usize {
    self.offset
  }

  pub fn is_eof(&self) -> bool {
    T <= self.offset
  }
}

#[cfg(test)]
#[path = "tests/page.rs"]
mod tests;
