use std::{
  ffi::OsStr,
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

#[derive(Debug)]
pub struct TableMetadata {
  id: TableId,
  name: String,
  path: PathBuf,
  compaction: Option<(TableId, PathBuf)>,
}
impl TableMetadata {
  pub const fn new(id: TableId, name: String, path: PathBuf) -> Self {
    Self {
      id,
      name,
      path,
      compaction: None,
    }
  }

  pub fn set_compaction(&mut self, metadata: &TableMetadata) {
    self.compaction = Some((metadata.get_id(), metadata.get_path().into()))
  }

  pub const fn get_compaction_id(&self) -> Option<TableId> {
    match &self.compaction {
      Some((id, _)) => Some(*id),
      None => None,
    }
  }
  pub fn get_compaction_metadata(&self) -> Option<TableMetadata> {
    let (id, path) = self.compaction.as_ref()?;
    Some(TableMetadata::new(*id, self.name.clone(), path.clone()))
  }

  pub fn to_vec(&self) -> Vec<u8> {
    let path = self.path.as_os_str().as_encoded_bytes();
    let path_len = path.len();
    let name_len = self.name.len();
    let compaction_len = 1
      + self
        .compaction
        .as_ref()
        .map(|(_, p)| p.as_os_str().len() + 4 + TABLE_ID_BYTES)
        .unwrap_or(0);
    let mut vec = vec![0; path_len + 4 + name_len + 2 + TABLE_ID_BYTES + compaction_len];
    let ptr = vec.as_mut_ptr();
    let mut offset = 0;
    unsafe {
      match &self.compaction {
        Some((id, path)) => {
          *ptr.add(offset) = 1;
          offset += 1;

          copy_nonoverlapping(id.to_le_bytes().as_ptr(), ptr.add(offset), TABLE_ID_BYTES);
          offset += TABLE_ID_BYTES;

          let path = path.as_os_str().as_encoded_bytes();
          copy_nonoverlapping(
            (path.len() as u32).to_le_bytes().as_ptr(),
            ptr.add(offset),
            4,
          );
          offset += 4;

          copy_nonoverlapping(path.as_ptr(), ptr.add(offset), path.len());
          offset += path.len();
        }
        None => {
          *ptr.add(offset) = 0;
          offset += 1;
        }
      }
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
    let mut offset = 1;
    unsafe {
      let compaction = match *ptr {
        0 => None,
        1 => {
          let id = TableId::from_le_bytes(
            (ptr.add(offset) as *const [u8; TABLE_ID_BYTES]).read(),
          );
          offset += TABLE_ID_BYTES;

          let len =
            u32::from_le_bytes((ptr.add(offset) as *const [u8; 4]).read()) as usize;
          offset += 4;

          let path =
            OsStr::from_encoded_bytes_unchecked(from_raw_parts(ptr.add(offset), len));
          offset += len;
          Some((id, PathBuf::from(path)))
        }
        _ => return Err(Error::InvalidFormat("metadata crashed.")),
      };

      let id =
        TableId::from_le_bytes((ptr.add(offset) as *const [u8; TABLE_ID_BYTES]).read());
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

      let path =
        OsStr::from_encoded_bytes_unchecked(from_raw_parts(ptr.add(offset), path_len));

      Ok(Self {
        id,
        name,
        path: PathBuf::from(path),
        compaction,
      })
    }
  }

  #[inline]
  pub const fn get_id(&self) -> TableId {
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

#[cfg(test)]
#[path = "tests/metadata.rs"]
mod test;
