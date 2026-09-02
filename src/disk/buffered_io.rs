use std::path::PathBuf;

use super::{AlignedArray, DirHandle, IOBackend, ALIGN};
use crate::{error::Result, utils::SBox, Error};

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
  pub fn append(&mut self, mut buf: &[u8]) -> Result {
    while !buf.is_empty() {
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
      buf = &buf[bytes..];
    }
    Ok(())
  }
  fn flush_buf(&mut self) -> Result {
    self
      .file
      .pwrite_exact(&*self.buffer, self.file_offset)
      .map_err(Error::IO)?;
    Ok(())
  }
  /**
   * Flush the buffered stream, sync the file, and finish the writer.
   *
   * The handle is consumed because this is the finalization step for the append
   * stream.
   */
  pub fn flush_all(&mut self) -> Result {
    self.flush_buf()?;
    self.file.fsync().map_err(Error::IO)?;
    Ok(())
  }
  pub const fn is_aligned(&self) -> bool {
    self.buffer_offset == ALIGN
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
  fn fill_buf(&mut self) -> Result {
    self
      .file
      .pread_exact(&mut *self.buffer, self.file_offset)
      .map_err(Error::IO)?;
    Ok(())
  }
  pub fn read_to_vec(&mut self, bytes: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0; bytes];
    self.read(&mut buf)?;
    Ok(buf)
  }
  pub fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
    let mut buf = [0; N];
    self.read(&mut buf)?;
    Ok(buf)
  }
  fn read(&mut self, mut buf: &mut [u8]) -> Result {
    while !buf.is_empty() {
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
      buf = &mut buf[bytes..];
    }
    Ok(())
  }
  pub const fn len(&self) -> u64 {
    self.file_len
  }
  pub fn truncate(&self) -> Result {
    self.base_dir.remove(&self.filename).map_err(Error::IO)
  }
  pub const fn get_offset(&self) -> u64 {
    self.file_offset + (self.buffer_offset as u64) - (ALIGN as u64)
  }
}

#[cfg(test)]
#[path = "tests/buffered_io.rs"]
mod tests;
