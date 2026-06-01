use crate::{disk::PAGE_SIZE, Error, Result};

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

  pub fn deserialize_byte(byte: u8) -> Result<Self> {
    match byte {
      1 => Ok(Self::Header),
      2 => Ok(Self::BTreeNode),
      3 => Ok(Self::DataEntry),
      4 => Ok(Self::DataChunk),
      _ => Err(Error::DeserializeError(None, None)),
    }
  }
}

pub trait TaggedObject {
  const TYPE: SerializeType;
}

pub const SERIALIZABLE_BYTES: usize = PAGE_SIZE - 1; // 1 byte reserved for SerializeType tag
