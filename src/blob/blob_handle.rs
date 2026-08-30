use std::{
  mem::transmute,
  sync::atomic::{AtomicU64, Ordering},
};

use crossbeam::utils::Backoff;

use super::{BlobLen, BlobMetadata, BlobOffset, BLOB_SIZE, BLOB_THRESHOLD};
use crate::{
  disk::{AlignedBuf, IOHandle, PendingIO},
  Error, Result,
};

/**
 * Handle for one fixed-size blob segment.
 *
 * A blob segment is preallocated when opened, writes its blob id into the file
 * header, and then hands out byte ranges by advancing an atomic reservation
 * offset. Since space is allocated up front, blob writes use the write-only IO
 * path rather than the dynamic alloc-and-write path.
 */
pub struct BlobHandle {
  io: IOHandle,
  metadata: BlobMetadata,
  reserved: AtomicU64,
}
impl BlobHandle {
  pub const fn new(io: IOHandle, metadata: BlobMetadata) -> Self {
    Self {
      io,
      metadata,
      reserved: AtomicU64::new(0),
    }
  }
  pub const fn metadata(&self) -> &BlobMetadata {
    &self.metadata
  }
  pub fn reserve(&self, size: BlobOffset) -> BlobReserved {
    let mut offset = self.reserved.load(Ordering::Acquire);
    let backoff = Backoff::new();
    loop {
      let new = offset + size;
      if new > BLOB_SIZE {
        return BlobReserved::Eof;
      }
      let Err(c) = self.reserved.compare_exchange_weak(
        offset,
        new,
        Ordering::Release,
        Ordering::Acquire,
      ) else {
        if new > BLOB_THRESHOLD {
          return BlobReserved::Last(offset);
        }
        return BlobReserved::Ok(offset);
      };

      offset = c;
      backoff.spin()
    }
  }

  /**
   * Read a blob range into an aligned owned buffer.
   *
   * Blob IO uses `AlignedBuf` as its value buffer type because reads and writes go
   * through the direct-IO-oriented path.
   */
  pub fn read_at(&self, offset: BlobOffset, len: BlobLen) -> Result<AlignedBuf> {
    let mut buf = AlignedBuf::new(len as usize);
    self
      .io
      .read(buf.get_mut_aligned_slice(), offset)
      .map_err(Error::IO)?;
    Ok(buf)
  }

  pub fn write(&self, data: &AlignedBuf, offset: BlobOffset) -> Result {
    // Blob segments are preallocated fixed-size files, so blob writes use the
    // write-only path. Submit the aligned physical slice required by direct IO.
    //
    // SAFETY: `write_only` requires a `'static` slice because the IO worker may run
    // later. This method immediately waits for completion, so the borrowed
    // `AlignedBuf` cannot be dropped before the worker finishes using the slice.
    let static_ref =
      unsafe { transmute::<&[u8], &'static [u8]>(data.get_aligned_slice()) };
    self.io.write_only(static_ref, offset).wait_flatten()
  }
  pub fn sync(&self) -> PendingIO {
    self.io.fdatasync()
  }
  pub fn truncate(&self) -> Result {
    self.io.truncate().map_err(Error::IO)
  }
}

pub enum BlobReserved {
  /**
   * Reservation succeeded and this segment can continue accepting reservations.
   */
  Ok(BlobOffset),
  /**
   * Reservation succeeded, but this segment reached its writable threshold.
   * The handle reports that no more reservations should be placed here.
   */
  Last(BlobOffset),
  /**
   * Reservation failed because the segment has no room for the requested range.
   */
  Eof,
}
