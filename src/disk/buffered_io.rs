use std::{io, path::PathBuf};

use super::{AlignedArray, DirHandle, IOBackend, ALIGN};
use crate::{utils::SBox, Error, Result};

/**
 * Buffered writer for direct-I/O append-style output.
 *
 * Callers append arbitrary byte slices, and the handle packs them into an
 * internally aligned buffer before issuing positioned writes. The mutable API
 * makes this a single-writer stream handle.
 */
pub struct AppendIOHandle {
  file: Box<dyn IOBackend>,
  buffer: AlignedArray,
  buffer_offset: usize,
  file_offset: u64,
}
impl AppendIOHandle {
  pub const fn new(file: Box<dyn IOBackend>) -> Self {
    Self {
      file,
      buffer: AlignedArray::new(),
      buffer_offset: 0,
      file_offset: 0,
    }
  }

  fn flush_buf(&mut self) -> io::Result<()> {
    self.file.pwrite_or_fail(&*self.buffer, self.file_offset)?;
    Ok(())
  }

  pub fn sync(&self) -> Result {
    self.file.fsync().map_err(Error::IO)
  }
}
impl io::Write for AppendIOHandle {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    debug_assert!(self.buffer_offset <= ALIGN);
    if self.buffer_offset == ALIGN {
      self.flush_buf()?;
      self.file_offset += ALIGN as u64;
      self.buffer_offset = 0;
    }

    let end = (self.buffer_offset + buf.len()).min(ALIGN);
    let bytes = end - self.buffer_offset;
    self.buffer[self.buffer_offset..end].copy_from_slice(&buf[..bytes]);
    self.buffer_offset = end;
    Ok(bytes)
  }

  fn flush(&mut self) -> io::Result<()> {
    self.flush_buf()
  }
}

/**
 * Buffered sequential reader for direct-I/O scan-style input.
 *
 * The handle reads through an internally aligned buffer and copies out exactly
 * the number of bytes requested by the caller. Like `AppendIOHandle`, it hides
 * the memory and disk alignment details from higher layers.
 */
pub struct ScanIOHandle {
  file: Box<dyn IOBackend>,
  buffer: AlignedArray,
  buffer_offset: usize,
  file_offset: u64,
  file_len: u64,
  base_dir: SBox<DirHandle>,
  filename: PathBuf,
}
impl ScanIOHandle {
  pub const fn new(
    file: Box<dyn IOBackend>,
    base_dir: SBox<DirHandle>,
    filename: PathBuf,
    file_len: u64,
  ) -> Self {
    Self {
      file,
      buffer: AlignedArray::new(),
      buffer_offset: ALIGN,
      file_offset: 0,
      file_len,
      base_dir,
      filename,
    }
  }
  fn fill_buf(&mut self) -> io::Result<()> {
    self
      .file
      .pread_or_fail(&mut *self.buffer, self.file_offset)?;
    Ok(())
  }

  pub const fn len(&self) -> u64 {
    self.file_len
  }
  pub fn truncate(&self) -> Result {
    self.base_dir.remove(&self.filename).map_err(Error::IO)
  }
  pub const fn get_offset(&self) -> u64 {
    self.file_offset + (self.buffer_offset as u64)
  }
}
impl io::Read for ScanIOHandle {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    debug_assert!(self.buffer_offset <= ALIGN);
    if self.buffer_offset == ALIGN {
      self.fill_buf()?;
      self.file_offset += ALIGN as u64;
      self.buffer_offset = 0;
    }

    let end = (self.buffer_offset + buf.len()).min(ALIGN);
    let bytes = end - self.buffer_offset;
    buf[..bytes].copy_from_slice(&self.buffer[self.buffer_offset..end]);
    self.buffer_offset = end;
    Ok(bytes)
  }
}
