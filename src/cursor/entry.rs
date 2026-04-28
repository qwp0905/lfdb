use std::collections::VecDeque;

use crate::{
  disk::{Page, Pointer, POINTER_BYTES},
  serialize::{
    Deserializable, Serializable, SerializeType, TypedObject, Viewable,
    SERIALIZABLE_BYTES,
  },
  wal::{TxId, TX_ID_BYTES},
  Error, Result,
};

pub const MAX_KEY: usize = 1 << 8;
pub const MAX_VALUE: usize = 1 << 16;

// Maximum inline value size for a single-version DataEntry.
// Overhead: next(8) + version_count(2) + version(8) + owner(8) + data_len(2) + type_byte(1) = 29
pub const LARGE_VALUE: usize =
  SERIALIZABLE_BYTES - ((TX_ID_BYTES << 1) + POINTER_BYTES + 2 + 2 + 1);
pub const CHUNK_SIZE: usize = SERIALIZABLE_BYTES - 2;

/**
 * Data: value fits inline in the DataEntry page.
 * Chunked: value exceeds LARGE_VALUE and is stored across separate DataChunk pages;
 *          only the page pointers are stored here.
 * Tombstone: marks the key as deleted.
 */
#[derive(Debug)]
pub enum RecordData {
  Data(Vec<u8>),
  Chunked(Vec<Pointer>),
  Tombstone,
}
impl RecordData {
  pub const fn len(&self) -> usize {
    match self {
      Self::Data(data) => 1 + 2 + data.len(),
      Self::Chunked(pointers) => 1 + 1 + POINTER_BYTES * pointers.len(),
      Self::Tombstone => 1,
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
  const fn byte_len(&self) -> usize {
    (POINTER_BYTES << 1) + self.data.len() // owner 8byte + version 8byte + data
  }
}

/**
 * MVCC version chain for a single key, stored as a linked list of pages.
 * When a page fills up with version records, overflow continues on the next page
 * pointed to by next. New versions are prepended so the most recent is always
 * at the front.
 */
#[derive(Debug)]
pub struct DataEntry {
  next: Option<Pointer>,
  versions: VecDeque<VersionRecord>,
}
impl DataEntry {
  pub const fn empty() -> Self {
    Self {
      next: None,
      versions: VecDeque::new(),
    }
  }
  pub fn init(version: VersionRecord) -> Self {
    let mut versions = VecDeque::new();
    versions.push_front(version);
    Self {
      next: None,
      versions,
    }
  }

  pub fn len(&self) -> usize {
    self.versions.len()
  }
  pub fn take_versions<'a>(&'a mut self) -> impl Iterator<Item = VersionRecord> + 'a {
    self.versions.drain(..)
  }
  pub fn set_versions(&mut self, new_versions: VecDeque<VersionRecord>) {
    self.versions = new_versions;
  }

  pub fn get_last_owner(&self) -> Option<TxId> {
    self.versions.front().map(|v| v.owner)
  }

  pub const fn get_next(&self) -> Option<Pointer> {
    self.next
  }
  pub const fn set_next(&mut self, ptr: Pointer) {
    self.next = Some(ptr);
  }

  pub fn append(&mut self, record: VersionRecord) {
    self.versions.push_front(record);
  }

  pub fn is_available(&self, record: &VersionRecord) -> bool {
    let byte_len =
      POINTER_BYTES + 2 + self.versions.iter().map(|v| v.byte_len()).sum::<usize>();
    record.byte_len() + byte_len <= SERIALIZABLE_BYTES
  }
}
impl TypedObject for DataEntry {
  fn get_type() -> SerializeType {
    SerializeType::DataEntry
  }
}
impl Serializable for DataEntry {
  fn write_at(&self, writer: &mut crate::disk::PageWriter) -> crate::Result {
    writer.write_u64(self.next.unwrap_or(0))?;
    writer.write_u16(self.versions.len() as u16)?;

    for record in &self.versions {
      writer.write_u64(record.version)?;
      writer.write_u64(record.owner)?;
      match &record.data {
        RecordData::Data(data) => {
          writer.write(&[0])?;
          writer.write_u16(data.len() as u16)?;
          writer.write(&data)?;
        }
        RecordData::Tombstone => writer.write(&[1])?,
        RecordData::Chunked(pointers) => {
          writer.write(&[2])?;
          writer.write_u8(pointers.len() as u8)?;
          for ptr in pointers {
            writer.write_u64(*ptr)?;
          }
        }
      }
    }
    Ok(())
  }
}
impl Deserializable for DataEntry {
  fn read_from(reader: &mut crate::disk::PageScanner) -> crate::Result<Self> {
    let next = reader.read_u64()?;
    let len = reader.read_u16()? as usize;
    let mut versions = VecDeque::with_capacity(len);
    for _ in 0..len {
      let version = reader.read_u64()?;
      let owner = reader.read_u64()?;
      let data = match reader.read()? {
        0 => {
          let l = reader.read_u16()? as usize;
          RecordData::Data(reader.read_n(l)?.to_vec())
        }
        1 => RecordData::Tombstone,
        2 => {
          let l = reader.read()? as usize;
          let mut pointers = Vec::with_capacity(l);
          for _ in 0..l {
            pointers.push(reader.read_u64()?);
          }
          RecordData::Chunked(pointers)
        }
        _ => return Err(Error::InvalidFormat("invalid type for data version record")),
      };
      versions.push_back(VersionRecord::new(owner, version, data))
    }
    Ok(Self {
      versions,
      next: (next != 0).then_some(next),
    })
  }
}

pub struct DataChunk {
  chunk: Vec<u8>,
}
impl DataChunk {
  pub const fn new(chunk: Vec<u8>) -> Self {
    Self { chunk }
  }
}
impl TypedObject for DataChunk {
  fn get_type() -> SerializeType {
    SerializeType::DataChunk
  }
}
impl Serializable for DataChunk {
  fn write_at(&self, writer: &mut crate::disk::PageWriter) -> crate::Result {
    writer.write_u16(self.chunk.len() as u16)?;
    writer.write(&self.chunk)?;
    Ok(())
  }
}
impl Deserializable for DataChunk {
  fn read_from(reader: &mut crate::disk::PageScanner) -> crate::Result<Self> {
    let len = reader.read_u16()? as usize;
    let chunk = reader.read_n(len)?.to_vec();
    Ok(Self { chunk })
  }
}

#[derive(Debug)]
enum RecordDataView {
  Data(usize, usize),
  Chunked(Vec<Pointer>),
  Tombstone,
}
#[derive(Debug)]
struct VersionRecordView {
  pub owner: TxId,
  pub version: TxId,
  pub data: RecordDataView,
}
impl VersionRecordView {
  const fn new(owner: TxId, version: TxId, data: RecordDataView) -> Self {
    Self {
      owner,
      version,
      data,
    }
  }
}

pub struct DataEntryView<'a> {
  page: &'a Page,
  next: Option<Pointer>,
  versions: Vec<VersionRecordView>,
}
impl<'a> TypedObject for DataEntryView<'a> {
  fn get_type() -> SerializeType {
    SerializeType::DataEntry
  }
}
impl<'a> Viewable<'a> for DataEntryView<'a> {
  fn read_from(
    page: &'a Page,
    reader: &mut crate::disk::PageScanner<'a>,
  ) -> Result<Self> {
    let next = reader.read_u64()?;
    let len = reader.read_u16()? as usize;
    let mut versions = Vec::with_capacity(len);
    for _ in 0..len {
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
          let l = reader.read()? as usize;
          let mut pointers = Vec::with_capacity(l);
          for _ in 0..l {
            pointers.push(reader.read_u64()?);
          }
          RecordDataView::Chunked(pointers)
        }
        _ => return Err(Error::InvalidFormat("invalid type for data version record")),
      };
      versions.push(VersionRecordView::new(owner, version, data));
    }
    Ok(Self {
      page,
      versions,
      next: (next != 0).then_some(next),
    })
  }
}
impl<'a> Serializable for DataEntryView<'a> {
  fn write_at(&self, writer: &mut crate::disk::PageWriter) -> Result {
    writer.write_u64(self.next.unwrap_or(0))?;
    writer.write_u16(self.versions.len() as u16)?;

    for record in &self.versions {
      writer.write_u64(record.version)?;
      writer.write_u64(record.owner)?;
      match &record.data {
        RecordDataView::Data(s, e) => {
          writer.write(&[0])?;
          writer.write_u16((e - s) as u16)?;
          writer.write(self.page.range(*s..*e))?;
        }
        RecordDataView::Tombstone => writer.write(&[1])?,
        RecordDataView::Chunked(pointers) => {
          writer.write(&[2])?;
          writer.write_u8(pointers.len() as u8)?;
          for ptr in pointers {
            writer.write_u64(*ptr)?;
          }
        }
      }
    }
    Ok(())
  }
}
pub enum RecordDataRef<'a> {
  Data(&'a [u8]),
  Chunked(&'a [Pointer]),
  Tombstone,
}
pub struct VersionRecordRef<'a> {
  pub owner: TxId,
  pub version: TxId,
  pub data: RecordDataRef<'a>,
}
impl<'a> DataEntryView<'a> {
  pub const fn get_next(&self) -> Option<Pointer> {
    self.next
  }

  #[allow(unused)]
  pub fn get_last_owner(&self) -> Option<TxId> {
    self.versions.first().map(|v| v.owner)
  }

  pub fn chunked_pointers(&self) -> impl Iterator<Item = &[Pointer]> + '_ {
    self.versions.iter().flat_map(|record| match &record.data {
      RecordDataView::Data(_, _) => None,
      RecordDataView::Chunked(pointers) => Some(pointers.as_slice()),
      RecordDataView::Tombstone => None,
    })
  }

  pub fn get_versions(&'a self) -> impl Iterator<Item = VersionRecordRef<'a>> + 'a {
    self.versions.iter().map(|record| VersionRecordRef {
      owner: record.owner,
      version: record.version,
      data: match &record.data {
        RecordDataView::Data(s, e) => RecordDataRef::Data(self.page.range(*s..*e)),
        RecordDataView::Chunked(pointers) => RecordDataRef::Chunked(pointers),
        RecordDataView::Tombstone => RecordDataRef::Tombstone,
      },
    })
  }

  pub fn is_empty(&self) -> bool {
    if self.versions.is_empty() {
      return true;
    }
    if self.versions.len() > 1 {
      return false;
    }
    if let RecordDataView::Tombstone = self.versions[0].data {
      return true;
    }
    false
  }
}

pub struct DataChunkView<'a> {
  page: &'a Page,
  start: usize,
  end: usize,
}
impl<'a> TypedObject for DataChunkView<'a> {
  fn get_type() -> SerializeType {
    SerializeType::DataChunk
  }
}
impl<'a> Viewable<'a> for DataChunkView<'a> {
  fn read_from(
    page: &'a Page,
    scanner: &mut crate::disk::PageScanner<'a>,
  ) -> Result<Self> {
    let len = scanner.read_u16()? as usize;
    let offset = scanner.advance(len)?;
    Ok(Self {
      page,
      start: offset,
      end: offset + len,
    })
  }
}
impl<'a> DataChunkView<'a> {
  pub fn get_data(&self) -> &[u8] {
    self.page.range(self.start..self.end)
  }
}

#[cfg(test)]
#[path = "tests/entry.rs"]
mod tests;
