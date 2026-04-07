use std::{
  path::{Path, PathBuf},
  ptr::copy_nonoverlapping,
  slice::from_raw_parts,
};

use crate::{Error, Result};

pub type TableId = u16;

pub struct TableMetadata {
  id: TableId,
  path: PathBuf,
}
impl TableMetadata {
  pub fn new(id: TableId, path: PathBuf) -> Self {
    Self { id, path }
  }

  pub fn to_vec(&self) -> Vec<u8> {
    let path = self.path.to_string_lossy();
    let len = path.len();
    let mut vec = vec![0; len + 2];
    let ptr = vec.as_mut_ptr();
    unsafe { copy_nonoverlapping(self.id.to_le_bytes().as_ptr(), ptr, 2) };
    unsafe { copy_nonoverlapping(path.as_ptr(), ptr.add(2), len) };
    vec
  }

  pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
    if bytes.len() < 2 {
      return Err(Error::InvalidFormat("metadata crashed."));
    }

    let ptr = bytes.as_ptr();
    let id = TableId::from_le_bytes(unsafe { (ptr as *const [u8; 2]).read() });
    let bytes = unsafe { from_raw_parts(ptr.add(2), bytes.len() - 2) };
    let name = unsafe { str::from_utf8_unchecked(bytes) };
    Ok(Self {
      id,
      path: PathBuf::from(name),
    })
  }

  pub fn get_id(&self) -> TableId {
    self.id
  }
  pub fn get_path(&self) -> &Path {
    &self.path
  }
}
