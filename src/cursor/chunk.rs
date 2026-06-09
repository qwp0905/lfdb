use super::ReadonlyPolicy;
use crate::{
  disk::{Page, Pointer},
  serialize::{Serializable, SerializeType, TypedObject, Viewable, SERIALIZABLE_BYTES},
  table::TableHandleRef,
  Result, VecRef,
};

pub const MAX_CHUNK_SIZE: usize = SERIALIZABLE_BYTES - 2;

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

pub struct DataChunkView<'a> {
  page: &'a Page,
  start: usize,
  end: usize,
}
impl<'a> DataChunkView<'a> {
  pub fn get_data(&self) -> &[u8] {
    self.page.range(self.start..self.end)
  }

  pub fn read_data<Policy: ReadonlyPolicy>(
    policy: Policy,
    pointers: &[Pointer],
    table: &TableHandleRef,
  ) -> Result<VecRef> {
    let mut data = Vec::new();

    for &ptr in pointers {
      let slot = policy.fetch_slot(ptr, table)?.for_read();
      let chunk: DataChunkView = slot.as_ref().view()?;
      data.extend_from_slice(chunk.get_data());
    }
    Ok(VecRef::copied(data))
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
