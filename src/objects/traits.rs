use crate::disk::PAGE_SIZE;

#[derive(Debug, Clone, Copy)]
pub enum SerializeType {
  Header,
  BTreeNode,
  DataEntry,
  DataChunk,
}
impl SerializeType {
  pub const fn type_byte(&self) -> u8 {
    match self {
      Self::Header => 1,
      Self::BTreeNode => 2,
      Self::DataEntry => 3,
      Self::DataChunk => 4,
    }
  }
}

pub trait TaggedObject {
  const TYPE: SerializeType;
}

pub const SERIALIZABLE_BYTES: usize = PAGE_SIZE - 1; // 1 byte reserved for SerializeType tag
