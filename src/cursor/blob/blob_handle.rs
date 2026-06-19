use std::{
  mem::transmute,
  sync::atomic::{AtomicU64, Ordering},
};

use crossbeam::utils::Backoff;

use super::{BlobId, BlobLen, BlobOffset, BLOB_ID_BYTES, BLOB_SIZE, BLOB_THRESHOLD};
use crate::{
  disk::{AsyncIO, IOHandle},
  Error, Result,
};

pub struct BlobHandle {
  id: BlobId,
  io: IOHandle,
  reserved: AtomicU64,
}
impl BlobHandle {
  pub fn replay(io: IOHandle) -> Result<Self> {
    let mut bytes = [0; BLOB_ID_BYTES];
    io.read(&mut bytes, 0).map_err(Error::IO)?;
    let id = BlobId::from_le_bytes(bytes);
    let reserved = io.len().map_err(Error::IO)?;
    Ok(Self {
      id,
      io,
      reserved: AtomicU64::new(reserved),
    })
  }
  pub fn open(id: BlobId, io: IOHandle) -> Result<Self> {
    io.fallocate(0, BLOB_SIZE).map_err(Error::IO)?;
    let this = Self {
      id,
      io,
      reserved: AtomicU64::new(BLOB_ID_BYTES as BlobOffset),
    };
    this.write(&this.get_id().to_le_bytes(), 0)?;
    Ok(this)
  }
  pub const fn get_id(&self) -> BlobId {
    self.id
  }
  pub fn reserve(&self, len: BlobOffset) -> BlobReserved {
    let mut offset = self.reserved.load(Ordering::Acquire);
    let backoff = Backoff::new();
    loop {
      let new = offset + len;
      if offset >= BLOB_THRESHOLD || new >= BLOB_SIZE {
        return BlobReserved::Eof;
      }
      let Err(c) = self.reserved.compare_exchange(
        offset,
        offset + len,
        Ordering::Release,
        Ordering::Acquire,
      ) else {
        if new >= BLOB_THRESHOLD {
          return BlobReserved::Last(offset);
        }
        return BlobReserved::Ok(offset);
      };

      offset = c;
      backoff.spin()
    }
  }
  pub fn read_at(&self, offset: BlobOffset, len: BlobLen) -> Result<Vec<u8>> {
    let mut data = vec![0; len as usize];
    self.io.read(&mut data, offset).map_err(Error::IO)?;
    Ok(data)
  }

  pub fn write(&self, data: &[u8], offset: BlobOffset) -> Result {
    // blob handle must call write only rather than alloc_and_write since it calls fallocate in constructor.
    let static_ref = unsafe { transmute::<&[u8], &'static [u8]>(data) };
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
