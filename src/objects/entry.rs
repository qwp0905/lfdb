use std::collections::VecDeque;

use super::{
  Deserializable, Serializable, SerializeType, TypedObject, VersionRecord,
  VersionRecordView, Viewable, SERIALIZABLE_BYTES,
};
use crate::{
  disk::{Page, PageScanner, Pointer, POINTER_BYTES},
  Result,
};

/**
 * On-page continuation of a single key's version chain.
 *
 * A leaf entry stores the latest version inline and points to `DataEntry` pages
 * for the remaining version records. `next` links to the next data-entry page
 * when this page cannot hold the whole chain. Version ordering is maintained by
 * the caller.
 */
#[derive(Debug)]
pub struct DataEntry {
  next: Option<Pointer>,
  versions: VecDeque<VersionRecord>,
}
impl DataEntry {
  pub fn init(version: VersionRecord, next: Option<Pointer>) -> Self {
    let mut versions = VecDeque::with_capacity(1);
    versions.push_front(version);
    Self { next, versions }
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
  pub const fn clear_next(&mut self) {
    self.next = None;
  }

  pub fn attach_front(&mut self, record: VersionRecord) {
    self.versions.push_front(record);
  }
  pub fn attach_back(&mut self, record: VersionRecord) {
    self.versions.push_back(record);
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
      versions.push_back(VersionRecord::deserialize_from(reader)?)
    }
    Ok(Self {
      versions,
      next: (next != 0).then_some(next),
    })
  }
}

/**
 * Zero-copy view of a serialized `DataEntry`.
 *
 * It provides read-only iteration over version-record views. Use `into_owned`
 * when the page contents need to be materialized for mutation and serialization.
 */
pub struct DataEntryView<'a> {
  page: &'a Page,
  next: Option<Pointer>,
  offset: usize,
  len: usize,
}
impl<'a> DataEntryView<'a> {
  pub fn find<P>(&self, mut predicate: P) -> Result<Option<VersionRecordView>>
  where
    P: FnMut(&VersionRecordView) -> bool,
  {
    let mut iter = self.get_versions();
    while let Some(record) = iter.try_next()? {
      if predicate(&record) {
        return Ok(Some(record));
      }
    }
    Ok(None)
  }

  pub fn get_versions(&self) -> DataEntryIter<'a> {
    let mut scanner = self.page.scanner();
    scanner.advance(self.offset).unwrap();
    DataEntryIter {
      scanner,
      pos: 0,
      len: self.len,
    }
  }

  #[allow(unused)]
  pub const fn len(&self) -> usize {
    self.len
  }

  pub const fn get_next(&self) -> Option<Pointer> {
    self.next
  }
}
impl<'a> TypedObject for DataEntryView<'a> {
  const TYPE: SerializeType = DataEntry::TYPE;
}
impl<'a> Viewable<'a> for DataEntryView<'a> {
  fn read_from(page: &'a Page, scanner: &mut PageScanner<'a>) -> crate::Result<Self> {
    let next = scanner.read_u64()?;
    let len = scanner.read_u16()? as usize;
    let offset = scanner.advance(0)?;
    Ok(Self {
      next: (next != 0).then_some(next),
      offset,
      len,
      page,
    })
  }
}

pub struct DataEntryIter<'a> {
  scanner: PageScanner<'a>,
  pos: usize,
  len: usize,
}
impl<'a> DataEntryIter<'a> {
  pub fn try_next(&mut self) -> Result<Option<VersionRecordView>> {
    if self.pos == self.len {
      return Ok(None);
    }

    let record = VersionRecordView::deserialize_from(&mut self.scanner)?;
    self.pos += 1;
    Ok(Some(record))
  }
}
#[cfg(test)]
#[path = "tests/entry.rs"]
mod tests;
