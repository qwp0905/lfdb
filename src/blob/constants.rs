use crate::{
  disk::ALIGN,
  objects::{LARGE_VALUE, MAX_VALUE},
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
pub const BLOB_SIZE: BlobOffset = (MAX_VALUE + ALIGN) as BlobOffset;
pub const BLOB_THRESHOLD: BlobOffset = BLOB_SIZE - LARGE_VALUE as BlobOffset;
