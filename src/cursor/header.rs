use crate::{
  disk::{PageScanner, PageWriter},
  serialize::{Serializable, SerializeType},
  Result,
};

/**
 * Page index 0 is permanently reserved for the tree header.
 */
pub const HEADER_INDEX: usize = 0;

/**
 * Persisted tree metadata: root page index and current height.
 */
#[derive(Debug)]
pub struct TreeHeader {
  root: usize,
  height: u16,
}

impl TreeHeader {
  pub fn new(root: usize) -> Self {
    Self { root, height: 0 }
  }

  pub fn get_root(&self) -> usize {
    self.root
  }

  pub fn set_root(&mut self, index: usize) {
    self.root = index
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
    writer.write_usize(self.root)?;
    writer.write_u16(self.height)?;
    Ok(())
  }

  fn read_from(reader: &mut PageScanner) -> Result<Self> {
    let root = reader.read_usize()?;
    let height = reader.read_u16()?;
    Ok(Self { root, height })
  }
}

#[cfg(test)]
#[path = "tests/header.rs"]
mod tests;
