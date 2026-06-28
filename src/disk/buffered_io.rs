use std::{mem::replace, path::PathBuf};

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
  filename: PathBuf,
}
impl AppendIOHandle {
  pub const fn new(file: Box<dyn IOBackend>, filename: PathBuf) -> Self {
    Self {
      file,
      filename,
      buffer: AlignedArray::new(),
      buffer_offset: 0,
      file_offset: 0,
    }
  }
  pub fn append(&mut self, mut buf: &[u8]) -> Result {
    loop {
      let end = self.buffer_offset + buf.len();
      if end <= ALIGN {
        self.buffer[replace(&mut self.buffer_offset, end)..end].copy_from_slice(buf);
        return Ok(());
      }

      let available = ALIGN - self.buffer_offset;
      self.buffer[self.buffer_offset..].copy_from_slice(&buf[..available]);
      self.flush_buf()?;
      buf = &buf[available..];
    }
  }
  fn flush_buf(&mut self) -> Result {
    self
      .file
      .pwrite_or_fail(&*self.buffer, self.file_offset)
      .map_err(Error::IO)?;
    self.file_offset += ALIGN as u64;
    self.buffer_offset = 0;
    Ok(())
  }
  /**
   * Flush the buffered stream, sync the file, and finish the writer.
   *
   * The handle is consumed because this is the finalization step for the append
   * stream.
   */
  pub fn flush(mut self) -> Result<PathBuf> {
    self.flush_buf()?;
    self.file.fsync().map_err(Error::IO)?;
    Ok(self.filename)
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
      .pread_or_fail(&mut *self.buffer, self.file_offset)
      .map_err(Error::IO)?;
    self.file_offset += ALIGN as u64;
    self.buffer_offset = 0;
    Ok(())
  }
  pub fn read_to_vec(&mut self, bytes: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0; bytes];
    self.read(&mut buf)?;
    Ok(buf)
  }
  pub fn read(&mut self, mut buf: &mut [u8]) -> Result {
    loop {
      let end = self.buffer_offset + buf.len();
      if end <= ALIGN {
        buf.copy_from_slice(&self.buffer[replace(&mut self.buffer_offset, end)..end]);
        return Ok(());
      }

      let available = ALIGN - self.buffer_offset;
      buf[..available].copy_from_slice(&self.buffer[self.buffer_offset..]);
      self.fill_buf()?;
      buf = &mut buf[available..];
    }
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
