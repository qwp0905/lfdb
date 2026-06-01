use std::collections::VecDeque;

use super::{VersionRecord, VersionRecordView};
use crate::{
  disk::{Page, Pointer, POINTER_BYTES},
  serialize::{
    Deserializable, Serializable, SerializeType, TypedObject, Viewable,
    SERIALIZABLE_BYTES,
  },
  wal::{TxId, TX_ID_BYTES},
};

pub const MAX_KEY: usize = 1 << 8;
pub const MAX_VALUE: usize = 1 << 16;

pub const CHUNK_SIZE: usize = SERIALIZABLE_BYTES - 2;

/**
 * MVCC version chain for a single key, stored as a linked list of pages.
 * When a page fills up with version records, overflow continues on the next page
 * pointed to by next. New versions are prepended so the most recent is always
 * at the front.
 */
#[derive(Debug)]
pub struct DataEntry {
  next: Option<Pointer>,
  versions: VecDeque<(TxId, VersionRecord)>,
}
impl DataEntry {
  pub const fn empty() -> Self {
    Self {
      next: None,
      versions: VecDeque::new(),
    }
  }
  pub fn init(version: VersionRecord, reclaimer: TxId) -> Self {
    let mut versions = VecDeque::with_capacity(1);
    versions.push_front((reclaimer, version));
    Self {
      next: None,
      versions,
    }
  }

  pub fn get_last(&self) -> Option<&(TxId, VersionRecord)> {
    self.versions.front()
  }

  pub fn len(&self) -> usize {
    self.versions.len()
  }
  pub fn take_versions<'a>(
    &'a mut self,
  ) -> impl Iterator<Item = (TxId, VersionRecord)> + 'a {
    self.versions.drain(..)
  }
  pub fn set_versions(&mut self, new_versions: VecDeque<(TxId, VersionRecord)>) {
    self.versions = new_versions;
  }

  pub const fn get_next(&self) -> Option<Pointer> {
    self.next
  }
  pub const fn set_next(&mut self, ptr: Pointer) {
    self.next = Some(ptr);
  }

  pub fn append(&mut self, record: VersionRecord, reclaimer: TxId) {
    self.versions.push_front((reclaimer, record));
  }

  pub fn is_available(&self, record: &VersionRecord) -> bool {
    let byte_len = (POINTER_BYTES << 1)
      + 2
      + self
        .versions
        .iter()
        .map(|(_, v)| TX_ID_BYTES + v.byte_len())
        .sum::<usize>();
    record.byte_len() + byte_len + TX_ID_BYTES <= SERIALIZABLE_BYTES
  }
}
impl TypedObject for DataEntry {
  const TYPE: SerializeType = SerializeType::DataEntry;
}
impl Serializable for DataEntry {
  fn write_at(&self, writer: &mut crate::disk::PageWriter) -> crate::Result {
    writer.write_u64(self.next.unwrap_or(0))?;
    writer.write_u16(self.versions.len() as u16)?;

    for (r, record) in &self.versions {
      writer.write_u64(*r)?;
      record.serialize_to(writer)?;
    }
    Ok(())
  }
}
impl Deserializable for DataEntry {
  fn read_from(reader: &mut crate::disk::PageScanner) -> crate::Result<Self> {
    let next = reader.read_u64()?;
    let len = reader.read_u16()? as usize;
    let mut versions = VecDeque::with_capacity(len + 1);
    for _ in 0..len {
      let reclaimer = reader.read_u64()?;
      versions.push_back((reclaimer, VersionRecord::deserialize_from(reader)?));
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
  const TYPE: SerializeType = SerializeType::DataChunk;
}
impl Serializable for DataChunk {
  fn write_at(&self, writer: &mut crate::disk::PageWriter) -> crate::Result {
    writer.write_u16(self.chunk.len() as u16)?;
    writer.write(&self.chunk)?;
    Ok(())
  }
}

pub struct DataEntryView {
  next: Option<Pointer>,
  versions: Vec<(TxId, VersionRecordView)>,
}
impl DataEntryView {
  pub fn find<P>(&self, predicate: P) -> Option<&VersionRecordView>
  where
    P: FnMut(&&VersionRecordView) -> bool,
  {
    self.versions.iter().map(|(_, v)| v).find(predicate)
  }

  pub fn get_versions(&self) -> impl Iterator<Item = &'_ VersionRecordView> + '_ {
    self.versions.iter().map(|(_, v)| v)
  }

  #[allow(unused)]
  pub const fn len(&self) -> usize {
    self.versions.len()
  }

  pub const fn get_next(&self) -> Option<Pointer> {
    self.next
  }
}
impl TypedObject for DataEntryView {
  const TYPE: SerializeType = DataEntry::TYPE;
}
impl Deserializable for DataEntryView {
  fn read_from(reader: &mut crate::disk::PageScanner) -> crate::Result<Self> {
    let next = reader.read_u64()?;
    let len = reader.read_u16()? as usize;
    let mut versions = Vec::with_capacity(len);
    for _ in 0..len {
      let reclaimer = reader.read_u64()?;
      versions.push((reclaimer, VersionRecordView::deserialize_from(reader)?));
    }
    Ok(Self {
      versions,
      next: (next != 0).then_some(next),
    })
  }
}

pub struct DataChunkView<'a> {
  page: &'a Page,
  start: usize,
  end: usize,
}
impl<'a> DataChunkView<'a> {
  pub fn get_data(&self) -> &[u8] {
    self.page.range(self.start..self.end)
  }
}
impl<'a> TypedObject for DataChunkView<'a> {
  const TYPE: SerializeType = DataChunk::TYPE;
}
impl<'a> Viewable<'a> for DataChunkView<'a> {
  fn read_from(
    page: &'a Page,
    reader: &mut crate::disk::PageScanner<'a>,
  ) -> crate::Result<Self> {
    let len = reader.read_u16()? as usize;
    let offset = reader.advance(len)?;
    Ok(Self {
      page,
      start: offset,
      end: offset + len,
    })
  }
}

#[cfg(test)]
#[path = "tests/entry.rs"]
mod tests;
