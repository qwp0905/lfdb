use std::{
  mem::transmute,
  sync::atomic::{AtomicU64, Ordering},
};

use crossbeam::utils::Backoff;

use super::{BlobId, BlobLen, BlobOffset, BLOB_ID_BYTES, BLOB_SIZE, BLOB_THRESHOLD};
use crate::{
  disk::{AlignedBuf, AsyncIO, IOHandle, ALIGN},
  Error, Result,
};

pub struct BlobHandle {
  id: BlobId,
  io: IOHandle,
  reserved: AtomicU64,
}
impl BlobHandle {
  pub fn replay(io: IOHandle) -> Result<Self> {
    let mut bytes = AlignedBuf::new(BLOB_ID_BYTES);
    io.read(bytes.get_mut_aligned_slice(), 0)
      .map_err(Error::IO)?;
    let id = BlobId::from_le_bytes(unsafe { bytes.as_ptr().cast::<[u8; _]>().read() });
    let reserved = io.len().map_err(Error::IO)?;
    Ok(Self {
      id,
      io,
      reserved: AtomicU64::new(reserved),
    })
  }
  pub fn open(id: BlobId, io: IOHandle) -> Result<Self> {
    io.fallocate(0, BLOB_SIZE).map_err(Error::IO)?;
    let mut buf = AlignedBuf::new(BLOB_ID_BYTES);
    debug_assert_eq!(buf.size(), ALIGN);
    let this = Self {
      id,
      io,
      reserved: AtomicU64::new(buf.size() as BlobOffset),
    };
    buf
      .as_mut_slice()
      .copy_from_slice(&this.get_id().to_le_bytes());
    this.write(&buf, 0)?;
    Ok(this)
  }
  pub const fn get_id(&self) -> BlobId {
    self.id
  }
  pub fn reserve(&self, size: BlobOffset) -> BlobReserved {
    let mut offset = self.reserved.load(Ordering::Acquire);
    let backoff = Backoff::new();
    loop {
      let new = offset + size;
      if new > BLOB_SIZE {
        return BlobReserved::Eof;
      }
      let Err(c) =
        self
          .reserved
          .compare_exchange(offset, new, Ordering::Release, Ordering::Acquire)
      else {
        if new > BLOB_THRESHOLD {
          return BlobReserved::Last(offset);
        }
        return BlobReserved::Ok(offset);
      };

      offset = c;
      backoff.spin()
    }
  }
  pub fn read_at(&self, offset: BlobOffset, len: BlobLen) -> Result<AlignedBuf> {
    let mut buf = AlignedBuf::new(len as usize);
    self
      .io
      .read(buf.get_mut_aligned_slice(), offset)
      .map_err(Error::IO)?;
    Ok(buf)
  }

  pub fn write(&self, data: &AlignedBuf, offset: BlobOffset) -> Result {
    // blob handle must call write only rather than alloc_and_write since it calls fallocate in constructor.
    let static_ref =
      unsafe { transmute::<&[u8], &'static [u8]>(data.get_aligned_slice()) };
    self
      .io
      .write_only(static_ref, offset)
      .wait()
      .unwrap()
      .map_err(Error::IO)
  }
  pub fn sync(&self) -> AsyncIO {
    AsyncIO::new(self.io.fdatasync())
  }
  pub fn truncate(&self) -> Result {
    self.io.truncate().map_err(Error::IO)
  }
}

pub enum BlobReserved {
  Ok(BlobOffset),
  Last(BlobOffset),
  Eof,
}
