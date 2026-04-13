use crate::{
  disk::{PageScanner, PageWriter, Pointer, POINTER_BYTES},
  serialize::{Serializable, SerializeType},
  Result,
};

pub const HEADER_POINTER: Pointer = 0;

/**
 * Persisted tree metadata: root page index and current height.
 */
#[derive(Debug)]
pub struct TreeHeader {
  root: Pointer,
  height: u16,
}

impl TreeHeader {
  pub fn new(root: Pointer) -> Self {
    Self { root, height: 0 }
  }

  pub fn get_root(&self) -> Pointer {
    self.root
  }

  pub fn set_root(&mut self, pointer: Pointer) {
    self.root = pointer
  }
  pub fn increase_height(&mut self) {
    self.height += 1;
  }
  pub fn get_height(&self) -> u16 {
    self.height
  }
}

impl Serializable for TreeHeader {
  fn get_type() -> SerializeType {
    SerializeType::Header
  }

  fn write_at(&self, writer: &mut PageWriter) -> Result {
    writer.write(&self.root.to_le_bytes())?;
    writer.write_u16(self.height)?;
    Ok(())
  }

  fn read_from(reader: &mut PageScanner) -> Result<Self> {
    let root = Pointer::from_le_bytes(reader.read_const_n::<POINTER_BYTES>()?);
    let height = reader.read_u16()?;
    Ok(Self { root, height })
  }
}

#[cfg(test)]
#[path = "tests/header.rs"]
mod tests;
