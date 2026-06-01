use super::{SerializeType, TaggedObject};
use crate::{
  disk::{PageScanner, PageWriter, Pointer},
  Result,
};

/**
 * Persisted tree metadata: root page index and current height.
 */
#[derive(Debug)]
pub struct TreeHeader {
  root: Pointer,
  height: u16,
}

impl TreeHeader {
  pub const fn new(root: Pointer) -> Self {
    Self { root, height: 0 }
  }

  pub const fn get_root(&self) -> Pointer {
    self.root
  }

  pub const fn set_root(&mut self, pointer: Pointer) {
    self.root = pointer
  }
  pub const fn increase_height(&mut self) {
    self.height += 1;
  }
  pub const fn get_height(&self) -> u16 {
    self.height
  }
  pub fn write_at(&self, writer: &mut PageWriter) -> Result {
    writer.write_u64(self.root)?;
    writer.write_u16(self.height)?;
    Ok(())
  }
  pub fn read_from(reader: &mut PageScanner) -> Result<Self> {
    let root = reader.read_u64()?;
    let height = reader.read_u16()?;
    Ok(Self { root, height })
  }
}
impl TaggedObject for TreeHeader {
  const TYPE: SerializeType = SerializeType::Header;
}

#[cfg(test)]
#[path = "tests/header.rs"]
mod tests;
