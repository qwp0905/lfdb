use std::{
  path::{Path, PathBuf},
  ptr::copy_nonoverlapping,
  slice::from_raw_parts,
  sync::atomic::AtomicU32,
};

use crate::{Error, Result};

pub type TableId = u32;
pub const TABLE_ID_BYTES: usize = TableId::BITS as usize >> 3;
pub type AtomicTableId = AtomicU32;

pub const MAX_TABLE_NAME_LEN: usize = 256 as usize;

pub struct TableMetadata {
  id: TableId,
  name: String,
  path: PathBuf,
}
impl TableMetadata {
  pub fn new(id: TableId, name: String, path: PathBuf) -> Self {
    Self { id, name, path }
  }

  pub fn to_vec(&self) -> Vec<u8> {
    let path = self.path.to_string_lossy();
    let path_len = path.len();
    let name_len = self.name.len();
    let mut vec = vec![0; path_len + 4 + name_len + 2 + TABLE_ID_BYTES];
    let ptr = vec.as_mut_ptr();
    let mut offset = 0;
    unsafe {
      copy_nonoverlapping(
        self.id.to_le_bytes().as_ptr(),
        ptr.add(offset),
        TABLE_ID_BYTES,
      );
      offset += TABLE_ID_BYTES;
      copy_nonoverlapping((name_len as u16).to_le_bytes().as_ptr(), ptr.add(offset), 2);
      offset += 2;
      copy_nonoverlapping(self.name.as_ptr(), ptr.add(offset), name_len);
      offset += name_len;

      copy_nonoverlapping((path_len as u32).to_le_bytes().as_ptr(), ptr.add(offset), 4);
      offset += 4;

      copy_nonoverlapping(path.as_ptr(), ptr.add(offset), path_len);
    };
    vec
  }

  pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
    let len = bytes.len();
    if len < TABLE_ID_BYTES + 2 + 4 {
      return Err(Error::InvalidFormat("metadata crashed."));
    }

    let ptr = bytes.as_ptr();
    let mut offset = 0;
    unsafe {
      let id = TableId::from_le_bytes((ptr as *const [u8; TABLE_ID_BYTES]).read());
      offset += TABLE_ID_BYTES;

      let name_len =
        u16::from_le_bytes((ptr.add(offset) as *const [u8; 2]).read()) as usize;
      offset += 2;
      if len < offset + name_len + 4 {
        return Err(Error::InvalidFormat("metadata crashed."));
      }

      let name =
        str::from_utf8_unchecked(from_raw_parts(ptr.add(offset), name_len)).to_string();
      offset += name_len;

      let path_len =
        u32::from_le_bytes((ptr.add(offset) as *const [u8; 4]).read()) as usize;
      offset += 4;

      if len < offset + path_len {
        return Err(Error::InvalidFormat("metadata crashed."));
      }

      let path = str::from_utf8_unchecked(from_raw_parts(ptr.add(offset), path_len));

      Ok(Self {
        id,
        name,
        path: PathBuf::from(path),
      })
    }
  }

  #[inline]
  pub fn get_id(&self) -> TableId {
    self.id
  }
  #[inline]
  pub fn get_path(&self) -> &Path {
    &self.path
  }
  #[inline]
  pub fn get_name(&self) -> &str {
    &self.name
  }
}
