use std::ops::Range;

use crate::{
  blob::{BlobId, BlobLen, BlobOffset, BLOB_ID_BYTES, BLOB_LEN_BYTES, BLOB_OFFSET_BYTES},
  disk::Page,
  wal::{TxId, TX_ID_BYTES},
  Error,
};

#[derive(Debug)]
pub enum RecordData {
  /**
   * Value bytes stored inline with this record.
   */
  Data(Vec<u8>),
  /**
   * Value bytes stored in blob storage, referenced by blob id, offset, and
   * logical length.
   */
  Blob(BlobId, BlobOffset, BlobLen),
  /**
   * Delete marker for the key.
   */
  Tombstone,
}
impl RecordData {
  const fn byte_len(&self) -> usize {
    1 + match self {
      RecordData::Data(data) => 2 + data.len(),
      RecordData::Blob(_, _, _) => BLOB_ID_BYTES + BLOB_OFFSET_BYTES + BLOB_LEN_BYTES,
      RecordData::Tombstone => 0,
    }
  }
}

/**
 * On-page MVCC version record.
 *
 * A version record stores the writer transaction, visibility version, record id,
 * and either inline value bytes, a blob location, or a tombstone. Leaf entries
 * and data-entry version chains share this format.
 *
 *  A record is uniquely identified by `(owner, record_id)`. The record id is
 * local to the owner transaction and may repeat across transactions.
 */
#[derive(Debug)]
pub struct VersionRecord {
  /**
   * The transaction that wrote this record.
   */
  pub owner: TxId,
  /**
   * The next transaction id at the moment this record is written to a
   * page. It forms the visibility boundary: transactions that already started
   * before that boundary should not observe the new version, while later
   * transactions may.
   */
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
    Self::RESERVED_BYTES + self.data.byte_len()
  }
  /**
   * owner 8byte + version 8byte + record id
   * */
  pub const RESERVED_BYTES: usize = (TX_ID_BYTES << 1);

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
  Data(Range<usize>),
  Blob(BlobId, BlobOffset, BlobLen),
  Tombstone,
}
impl RecordDataView {
  pub const fn is_tombstone(&self) -> bool {
    matches!(self, RecordDataView::Tombstone)
  }
}

/**
 * Zero-copy view of a serialized `VersionRecord`.
 *
 * Inline data is represented as a byte range inside the page. Blob and tombstone
 * variants are already self-contained. Use `into_owned_with` when an owned
 * `VersionRecord` is needed.
 */
#[derive(Debug)]
pub struct VersionRecordView {
  pub owner: TxId,
  pub version: TxId,
  pub data: RecordDataView,
}
impl VersionRecordView {
  pub const fn new(owner: TxId, version: TxId, data: RecordDataView) -> Self {
    Self {
      owner,
      version,
      data,
    }
  }

  pub fn into_owned_with(self, page: &Page) -> VersionRecord {
    VersionRecord::new(
      self.owner,
      self.version,
      match self.data {
        RecordDataView::Data(range) => RecordData::Data(page.copy_range(range)),
        RecordDataView::Blob(i, o, l) => RecordData::Blob(i, o, l),
        RecordDataView::Tombstone => RecordData::Tombstone,
      },
    )
  }

  pub fn deserialize_from(reader: &mut crate::disk::PageScanner) -> crate::Result<Self> {
    let version = reader.read_u64()?;
    let owner = reader.read_u64()?;
    let data = match reader.read()? {
      0 => {
        let l = reader.read_u16()? as usize;
        let offset = reader.advance(l)?;
        RecordDataView::Data(offset..(offset + l))
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
