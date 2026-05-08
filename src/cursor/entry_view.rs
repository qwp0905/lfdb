use crate::{
  disk::{Page, Pointer},
  serialize::{Deserializable, SerializeType, TypedObject, Viewable},
  wal::TxId,
  Error,
};

pub enum RecordDataView {
  Data(usize, usize),
  Chunked(Vec<Pointer>),
  Tombstone,
}

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

  pub const fn get_next(&self) -> Option<Pointer> {
    self.next
  }
}
impl TypedObject for DataEntryView {
  fn get_type() -> SerializeType {
    SerializeType::DataEntry
  }
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
  fn get_type() -> SerializeType {
    SerializeType::DataChunk
  }
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
