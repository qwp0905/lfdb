use std::{ffi::OsStr, path::PathBuf};

use crate::{
  objects::{LARGE_VALUE, MAX_VALUE},
  utils::{OffsetReader, OffsetWriter},
};

pub type BlobId = u64;
pub const BLOB_ID_BYTES: usize = BlobId::BITS as usize >> 3;

pub type BlobOffset = u64;
pub const BLOB_OFFSET_BYTES: usize = BlobOffset::BITS as usize >> 3;

pub type BlobLen = u32;
pub const BLOB_LEN_BYTES: usize = BlobLen::BITS as usize >> 3;

/**
 * Blob sizing defaults derived from the maximum supported value size.
 */
pub const BLOB_SIZE: BlobOffset = MAX_VALUE as BlobOffset;
pub const BLOB_THRESHOLD: BlobOffset = BLOB_SIZE - LARGE_VALUE as BlobOffset;

#[derive(Debug)]
pub struct BlobMetadata {
  id: BlobId,
  filename: PathBuf,
}
impl BlobMetadata {
  pub const fn new(id: BlobId, filename: PathBuf) -> Self {
    Self { id, filename }
  }
  pub const fn get_id(&self) -> BlobId {
    self.id
  }
  pub const fn get_filename(&self) -> &PathBuf {
    &self.filename
  }

  pub fn byte_len(&self) -> usize {
    BLOB_ID_BYTES + self.filename.as_os_str().len()
  }
  pub fn write_at(&self, writer: &mut OffsetWriter) {
    writer.write_u64(self.id);
    writer.write(self.filename.as_os_str().as_encoded_bytes());
  }
  pub fn read_from(reader: &mut OffsetReader) -> Option<Self> {
    let id = reader.read_u64()?;
    let bytes = reader.read_all();
    let filename = unsafe { OsStr::from_encoded_bytes_unchecked(bytes) };
    Some(Self {
      id,
      filename: filename.into(),
    })
  }
}
impl Clone for BlobMetadata {
  fn clone(&self) -> Self {
    Self {
      id: self.id,
      filename: self.filename.clone(),
    }
  }
}
