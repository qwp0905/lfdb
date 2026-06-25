use std::{
  marker::PhantomData, mem::replace, ptr::copy_nonoverlapping, slice::from_raw_parts,
};

/**
 * Sequential reader over a byte buffer.
 *
 * `OffsetReader` keeps the current offset and advances it after each successful
 * read. It centralizes bounds checks for serialization/deserialization code so
 * callers do not have to manually compute slice ranges at every step.
 */
pub struct OffsetReader<'a> {
  ptr: *const u8,
  offset: usize,
  len: usize,
  _marker: PhantomData<&'a [u8]>,
}
impl<'a> OffsetReader<'a> {
  pub const fn new(buf: &'a [u8]) -> Self {
    unsafe { Self::from_ptr(buf.as_ptr(), buf.len()) }
  }

  /**
   * Create a reader from a raw pointer and length.
   *
   * # Safety
   *
   * The caller must guarantee that `ptr` points to a valid readable memory range
   * of at least `len` bytes. `OffsetReader` does not validate that the pointer is
   * non-null, aligned, allocated, or large enough; it only checks accesses against
   * the provided `len`.
   */
  pub const unsafe fn from_ptr(ptr: *const u8, len: usize) -> Self {
    Self {
      ptr,
      offset: 0,
      len,
      _marker: PhantomData,
    }
  }
  pub const fn advance(&mut self, offset: usize) -> Option<usize> {
    let end = self.offset + offset;
    if end > self.len {
      return None;
    }
    Some(replace(&mut self.offset, end))
  }
  pub const fn read_byte(&mut self) -> Option<u8> {
    if self.offset >= self.len {
      return None;
    }
    let v = unsafe { self.ptr.add(self.offset).read() };
    self.offset += 1;
    Some(v)
  }
  pub const fn read(&mut self, len: usize) -> Option<&'a [u8]> {
    let end = self.offset + len;
    if end > self.len {
      return None;
    }
    let buf = unsafe { from_raw_parts(self.ptr.add(self.offset), len) };
    self.offset = end;
    Some(buf)
  }
  /**
   * Read a fixed-size byte array and advance the offset.
   *
   * This is mainly used by integer readers, which first read the raw bytes and
   * then decode them with the module's fixed byte order.
   */
  pub const fn read_array<const N: usize>(&mut self) -> Option<[u8; N]> {
    if self.offset + N > self.len {
      return None;
    }
    let buf = unsafe { (self.ptr.add(self.offset) as *const [u8; N]).read() };
    self.offset += N;
    Some(buf)
  }
  pub const fn read_u64(&mut self) -> Option<u64> {
    match self.read_array() {
      Some(buf) => Some(u64::from_le_bytes(buf)),
      None => None,
    }
  }
  pub const fn read_u32(&mut self) -> Option<u32> {
    match self.read_array() {
      Some(buf) => Some(u32::from_le_bytes(buf)),
      None => None,
    }
  }
  pub const fn read_u16(&mut self) -> Option<u16> {
    match self.read_array() {
      Some(buf) => Some(u16::from_le_bytes(buf)),
      None => None,
    }
  }
  pub const fn read_all(&mut self) -> &'a [u8] {
    debug_assert!(self.offset <= self.len);
    let buf =
      unsafe { from_raw_parts(self.ptr.add(self.offset), self.len - self.offset) };
    self.offset = self.len;
    buf
  }
  pub const fn is_eof(&self) -> bool {
    self.len <= self.offset
  }
}

/**
 * Sequential writer over a byte buffer.
 *
 * `OffsetWriter` writes at the current offset, advances after each successful
 * write, and returns `false` when the target buffer does not have enough
 * remaining space.
 */
pub struct OffsetWriter<'a> {
  ptr: *mut u8,
  offset: usize,
  len: usize,
  _marker: PhantomData<&'a mut [u8]>,
}
impl<'a> OffsetWriter<'a> {
  pub const fn new(buf: &'a mut [u8]) -> Self {
    unsafe { Self::from_ptr(buf.as_mut_ptr(), buf.len()) }
  }

  /**
   * Create a writer from a raw pointer and length.
   *
   * # Safety
   *
   * The caller must guarantee that `ptr` points to a valid writable memory range
   * of at least `len` bytes and that writing through it does not violate aliasing
   * rules. `OffsetWriter` only checks writes against the provided `len`.
   */
  pub const unsafe fn from_ptr(ptr: *mut u8, len: usize) -> Self {
    Self {
      ptr,
      offset: 0,
      len,
      _marker: PhantomData,
    }
  }
  pub const fn write(&mut self, buf: &[u8]) -> bool {
    let len = buf.len();
    let end = self.offset + len;
    if end > self.len {
      return false;
    };
    unsafe { copy_nonoverlapping(buf.as_ptr(), self.ptr.add(self.offset), len) };
    self.offset = end;
    true
  }
  pub const fn write_u64(&mut self, value: u64) -> bool {
    self.write(&value.to_le_bytes())
  }
  pub const fn write_u32(&mut self, value: u32) -> bool {
    self.write(&value.to_le_bytes())
  }
  pub const fn write_u16(&mut self, value: u16) -> bool {
    self.write(&value.to_le_bytes())
  }
  pub const fn write_u8(&mut self, value: u8) -> bool {
    self.write(&[value])
  }
  pub const fn written_bytes(&self) -> usize {
    self.offset
  }
}

#[cfg(test)]
#[path = "tests/buffer.rs"]
mod tests;
