use super::{
  BlobId, BlobLen, BlobOffset, BLOB_ID_BYTES, BLOB_LEN_BYTES, BLOB_OFFSET_BYTES,
};

use crate::{disk::POINTER_BYTES, wal::TxId, Error};

/**
 * Data: value fits inline in the DataEntry page.
 * Chunked: value exceeds LARGE_VALUE and is stored across separate DataChunk pages;
 *          only the page pointers are stored here.
 * Tombstone: marks the key as deleted.
 */
#[derive(Debug)]
pub enum RecordData {
  Data(Vec<u8>),
  Blob(BlobId, BlobOffset, BlobLen),
  Tombstone,
}
impl RecordData {
  pub const fn len(&self) -> usize {
    1 + match self {
      RecordData::Data(data) => 2 + data.len(),
      RecordData::Blob(_, _, _) => BLOB_ID_BYTES + BLOB_OFFSET_BYTES + BLOB_LEN_BYTES,
      RecordData::Tombstone => 0,
    }
  }
}

/**
 * owner: tx_id of the transaction that wrote this version.
 * version: the global tx counter at insert time. Only transactions that started
 * at or after this value can see this version — ensuring writes become visible
 * only to transactions that begin after the insert.
 */
#[derive(Debug)]
pub struct VersionRecord {
  pub owner: TxId,
  pub version: TxId,
  pub data: RecordData,
}
impl VersionRecord {
  pub const fn new(owner: TxId, version: TxId, data: RecordData) -> Self {
    Self {
      owner,
      version,
      data,
    }
  }
  pub const fn byte_len(&self) -> usize {
    (POINTER_BYTES << 1) + self.data.len() // owner 8byte + version 8byte + data
  }

  pub fn serialize_to(&self, writer: &mut crate::disk::PageWriter) -> crate::Result {
    writer.write_u64(self.version)?;
    writer.write_u64(self.owner)?;
    match &self.data {
      RecordData::Data(data) => {
        writer.write(&[0])?;
        writer.write_u16(data.len() as u16)?;
        writer.write(data)?;
      }
      RecordData::Tombstone => writer.write(&[1])?,
      RecordData::Blob(id, offset, len) => {
        writer.write(&[2])?;
        writer.write_u64(*id)?;
        writer.write_u64(*offset)?;
        writer.write_u32(*len)?;
      }
    }
    Ok(())
  }

  pub fn deserialize_from(reader: &mut crate::disk::PageScanner) -> crate::Result<Self> {
    let version = reader.read_u64()?;
    let owner = reader.read_u64()?;
    let data = match reader.read()? {
      0 => {
        let l = reader.read_u16()? as usize;
        RecordData::Data(reader.read_n(l)?.to_vec())
      }
      1 => RecordData::Tombstone,
      2 => {
        let id = reader.read_u64()?;
        let offset = reader.read_u64()?;
        let len = reader.read_u32()?;
        RecordData::Blob(id, offset, len)
      }
      _ => return Err(Error::InvalidFormat("invalid type for data version record")),
    };
    Ok(Self::new(owner, version, data))
  }
}

#[derive(Debug)]
pub enum RecordDataView {
  Data(usize, usize),
  Blob(BlobId, BlobOffset, BlobLen),
  Tombstone,
}
impl RecordDataView {
  pub const fn is_tombstone(&self) -> bool {
    matches!(self, RecordDataView::Tombstone)
  }
}

#[derive(Debug)]
pub struct VersionRecordView {
  pub owner: TxId,
  pub version: TxId,
  pub data: RecordDataView,
}
impl VersionRecordView {
  pub fn new(owner: TxId, version: TxId, data: RecordDataView) -> Self {
    Self {
      owner,
      version,
      data,
    }
  }

  pub fn deserialize_from(reader: &mut crate::disk::PageScanner) -> crate::Result<Self> {
    let version = reader.read_u64()?;
    let owner = reader.read_u64()?;
    let data = match reader.read()? {
      0 => {
        let l = reader.read_u16()? as usize;
        let offset = reader.advance(l)?;
        RecordDataView::Data(offset, offset + l)
      }
      1 => RecordDataView::Tombstone,
      2 => {
        let id = reader.read_u64()?;
        let offset = reader.read_u64()?;
        let len = reader.read_u32()?;
        RecordDataView::Blob(id, offset, len)
      }
      _ => return Err(Error::InvalidFormat("invalid type for data version record")),
    };
    Ok(Self::new(owner, version, data))
  }
}
