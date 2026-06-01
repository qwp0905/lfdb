use super::{SerializeType, TaggedObject};
use crate::disk::Page;

pub struct DataChunk {
  chunk: Vec<u8>,
}
impl DataChunk {
  pub const fn new(chunk: Vec<u8>) -> Self {
    Self { chunk }
  }

  pub fn write_at(&self, writer: &mut crate::disk::PageWriter) -> crate::Result {
    writer.write_u16(self.chunk.len() as u16)?;
    writer.write(&self.chunk)?;
    Ok(())
  }

  pub fn read_from(reader: &mut crate::disk::PageScanner) -> crate::Result<Self> {
    let len = reader.read_u16()? as usize;
    let chunk = reader.read_n(len)?.to_vec();
    Ok(Self { chunk })
  }
}
impl TaggedObject for DataChunk {
  const TYPE: SerializeType = SerializeType::DataChunk;
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

  pub fn read_from(
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
impl<'a> TaggedObject for DataChunkView<'a> {
  const TYPE: SerializeType = DataChunk::TYPE;
}
