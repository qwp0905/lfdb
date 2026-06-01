use std::collections::VecDeque;

use super::{RecordData, RecordDataView, VersionRecord, VersionRecordView};
use crate::{
  disk::{Page, Pointer, POINTER_BYTES},
  serialize::{
    Deserializable, Serializable, SerializeType, TypedObject, Viewable,
    SERIALIZABLE_BYTES,
  },
  Error,
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
    let mut versions = VecDeque::with_capacity(1);
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
  const TYPE: SerializeType = SerializeType::DataEntry;
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
    let mut versions = VecDeque::with_capacity(len + 1);
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
  versions: Vec<VersionRecordView>,
}
impl DataEntryView {
  pub fn find<P>(&self, predicate: P) -> Option<&VersionRecordView>
  where
    P: FnMut(&&VersionRecordView) -> bool,
  {
    self.versions.iter().find(predicate)
  }

  pub fn get_versions(&self) -> impl Iterator<Item = &'_ VersionRecordView> + '_ {
    self.versions.iter()
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
      versions.push(VersionRecordView::new(owner, version, data))
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
