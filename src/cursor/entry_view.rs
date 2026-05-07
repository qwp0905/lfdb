use crate::{
  disk::{Page, Pointer, PAGE_SIZE},
  serialize::{Serializable, SerializeType, TypedObject, Viewable},
  utils::InlineVec,
  wal::TxId,
  Error,
};

enum RecordDataOffset {
  Data(usize, usize),
  Chunked(InlineVec<Pointer, 3>),
  Tombstone,
}
pub enum RecordDataView<'a> {
  Data(&'a [u8]),
  Chunked(&'a [Pointer]),
  Tombstone,
}
pub struct VersionRecordView<'a> {
  pub owner: TxId,
  pub version: TxId,
  pub data: RecordDataView<'a>,
}
pub struct DataEntryView<'a> {
  page: &'a Page<PAGE_SIZE>,
  owners: InlineVec<TxId, 3>,
  versions: InlineVec<TxId, 3>,
  data: Vec<RecordDataOffset>,
  next: Option<Pointer>,
}
impl<'a> TypedObject for DataEntryView<'a> {
  const TYPE: SerializeType = SerializeType::DataEntry;
}
impl<'a> Serializable for DataEntryView<'a> {
  fn write_at(&self, writer: &mut crate::disk::PageWriter) -> crate::Result {
    writer.write_u64(self.next.unwrap_or(0))?;
    writer.write_u16(self.versions.len() as u16)?;
    for i in 0..self.versions.len() {
      writer.write_u64(self.versions[i])?;
      writer.write_u64(self.owners[i])?;

      match &self.data[i] {
        RecordDataOffset::Data(s, e) => {
          writer.write(&[0])?;
          writer.write_u16((e - s) as u16)?;
          writer.write(self.page.range(*s..*e))?;
        }
        RecordDataOffset::Tombstone => writer.write(&[1])?,
        RecordDataOffset::Chunked(pointers) => {
          writer.write(&[2])?;
          writer.write_u8(pointers.len() as u8)?;
          for ptr in pointers.iter() {
            writer.write_u64(*ptr)?;
          }
        }
      }
    }
    Ok(())
  }
}
impl<'a> Viewable<'a> for DataEntryView<'a> {
  fn read_from(
    page: &'a Page<PAGE_SIZE>,
    reader: &mut crate::disk::PageScanner<'a>,
  ) -> crate::Result<Self> {
    let next = reader.read_u64()?;
    let len = reader.read_u16()? as usize;
    let mut versions = InlineVec::with_capacity(len);
    let mut owners = InlineVec::with_capacity(len);
    let mut data = Vec::with_capacity(len);
    for _ in 0..len {
      versions.push(reader.read_u64()?);
      owners.push(reader.read_u64()?);
      let record = match reader.read()? {
        0 => {
          let l = reader.read_u16()? as usize;
          let offset = reader.advance(l)?;
          RecordDataOffset::Data(offset, offset + l)
        }
        1 => RecordDataOffset::Tombstone,
        2 => {
          let l = reader.read()? as usize;
          let mut pointers = InlineVec::with_capacity(l);
          for _ in 0..l {
            pointers.push(reader.read_u64()?);
          }
          RecordDataOffset::Chunked(pointers)
        }
        _ => return Err(Error::InvalidFormat("invalid type for data version record")),
      };
      data.push(record);
    }
    Ok(Self {
      page,
      versions,
      data,
      owners,
      next: (next != 0).then_some(next),
    })
  }
}

impl<'a> DataEntryView<'a> {
  pub fn find<P>(&'a self, mut predict: P) -> Option<VersionRecordView<'a>>
  where
    P: FnMut(&TxId, &TxId) -> bool,
  {
    for i in 0..self.versions.len() {
      let owner = self.owners[i];
      let version = self.versions[i];
      if predict(&owner, &version) {
        return Some(VersionRecordView {
          owner,
          version,
          data: match &self.data[i] {
            RecordDataOffset::Data(s, e) => RecordDataView::Data(self.page.range(*s..*e)),
            RecordDataOffset::Chunked(pointers) => RecordDataView::Chunked(pointers),
            RecordDataOffset::Tombstone => RecordDataView::Tombstone,
          },
        });
      }
    }

    None
  }

  pub const fn get_next(&self) -> Option<Pointer> {
    self.next
  }
  pub fn is_empty(&self) -> bool {
    if self.versions.is_empty() {
      return true;
    }
    if self.versions.len() > 1 {
      return false;
    }
    if let RecordDataOffset::Tombstone = self.data[0] {
      return true;
    }
    false
  }
}

pub struct DataChunkView<'a> {
  page: &'a Page<PAGE_SIZE>,
  start: usize,
  end: usize,
}
impl<'a> DataChunkView<'a> {
  pub const fn get_data(&'a self) -> &'a [u8] {
    self.page.range(self.start..self.end)
  }
}
impl<'a> TypedObject for DataChunkView<'a> {
  const TYPE: SerializeType = SerializeType::DataChunk;
}
impl<'a> Viewable<'a> for DataChunkView<'a> {
  fn read_from(
    page: &'a Page<PAGE_SIZE>,
    scanner: &mut crate::disk::PageScanner<'a>,
  ) -> crate::Result<Self> {
    let len = scanner.read_u16()? as usize;
    let offset = scanner.advance(len)?;
    Ok(Self {
      page,
      start: offset,
      end: offset + len,
    })
  }
}
