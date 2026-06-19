use crate::{disk::POINTER_BYTES, serialize::SERIALIZABLE_BYTES, wal::TX_ID_BYTES};

pub const MAX_KEY: usize = 1 << 8;
pub const MAX_VALUE: usize = BLOB_SIZE as usize - BLOB_ID_BYTES;

// Maximum inline value size for a leaf entry.
pub const LARGE_VALUE: usize = ((SERIALIZABLE_BYTES - (1 + POINTER_BYTES + 2)) >> 1)
  - (MAX_KEY + POINTER_BYTES + 2 + (TX_ID_BYTES << 1) + 1 + 2);

pub type BlobId = u64;
pub const BLOB_ID_BYTES: usize = BlobId::BITS as usize >> 3;

pub type BlobOffset = u64;
pub const BLOB_OFFSET_BYTES: usize = BlobOffset::BITS as usize >> 3;

pub type BlobLen = u32;
pub const BLOB_LEN_BYTES: usize = BlobLen::BITS as usize >> 3;

pub const BLOB_SIZE: BlobOffset = 32 << 20;
pub const BLOB_THRESHOLD: BlobOffset = BLOB_SIZE - LARGE_VALUE as BlobOffset;
