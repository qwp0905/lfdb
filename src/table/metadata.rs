use std::{
  ffi::OsStr,
  path::{Path, PathBuf},
  sync::atomic::AtomicU32,
};

use super::TableName;
use crate::{
  utils::{OffsetReader, OffsetWriter},
  Error, Result,
};

pub type TableId = u32;
pub const TABLE_ID_BYTES: usize = TableId::BITS as usize >> 3;
pub type AtomicTableId = AtomicU32;

#[derive(Debug)]
pub struct TableMetadata {
  id: TableId,
  name: TableName,
  filename: PathBuf,
  compaction: Option<(TableId, PathBuf)>,
}
impl TableMetadata {
  pub const fn new(id: TableId, name: TableName, filename: PathBuf) -> Self {
    Self {
      id,
      name,
      filename,
      compaction: None,
    }
  }

  pub fn set_compaction(&mut self, metadata: &TableMetadata) {
    self.compaction = Some((metadata.get_id(), metadata.get_filename().into()))
  }

  pub const fn get_compaction_id(&self) -> Option<TableId> {
    match &self.compaction {
      Some((id, _)) => Some(*id),
      None => None,
    }
  }
  pub fn get_compaction_metadata(&self) -> Option<TableMetadata> {
    let (id, filename) = self.compaction.as_ref()?;
    Some(TableMetadata::new(*id, self.name.clone(), filename.clone()))
  }

  pub fn to_vec(&self) -> Vec<u8> {
    let filename = self.filename.as_os_str().as_encoded_bytes();
    let filename_len = filename.len();
    let name_len = self.name.len();
    let compaction_len = 1
      + self
        .compaction
        .as_ref()
        .map(|(_, p)| p.as_os_str().len() + 2 + TABLE_ID_BYTES)
        .unwrap_or(0);
    let mut vec =
      vec![0; filename_len + 2 + name_len + 2 + TABLE_ID_BYTES + compaction_len];

    let mut writer = OffsetWriter::new(&mut vec);
    match &self.compaction {
      Some((id, path)) => {
        writer.write_u8(1);
        writer.write_u32(*id);

        let path = path.as_os_str().as_encoded_bytes();
        writer.write_u16(path.len() as u16);
        writer.write(path);
      }
      None => {
        writer.write_u8(0);
      }
    };

    writer.write_u32(self.id);

    writer.write_u16(name_len as u16);
    writer.write(self.name.as_bytes());

    writer.write_u16(filename_len as u16);
    writer.write(filename);

    vec
  }

  fn read_from(bytes: &[u8]) -> Option<Self> {
    let mut reader = OffsetReader::new(bytes);
    let compaction = match reader.read_byte()? {
      0 => None,
      1 => {
        let id = reader.read_u32()?;
        let len = reader.read_u16()? as usize;
        let path = unsafe { OsStr::from_encoded_bytes_unchecked(reader.read(len)?) };
        Some((id, PathBuf::from(path)))
      }
      _ => return None,
    };

    let id = reader.read_u32()?;

    let name_len = reader.read_u16()? as usize;
    let name = TableName::from_str_unchecked(unsafe {
      str::from_utf8_unchecked(reader.read(name_len)?)
    });

    let path_len = reader.read_u16()? as usize;
    let path = unsafe { OsStr::from_encoded_bytes_unchecked(reader.read(path_len)?) };

    Some(Self {
      id,
      name,
      filename: PathBuf::from(path),
      compaction,
    })
  }

  pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
    match Self::read_from(bytes) {
      Some(v) => Ok(v),
      None => Err(Error::InvalidFormat("metadata crashed.")),
    }
  }

  #[inline]
  pub const fn get_id(&self) -> TableId {
    self.id
  }
  #[inline]
  pub fn get_filename(&self) -> &Path {
    &self.filename
  }
  #[inline]
  pub fn get_name(&self) -> &TableName {
    &self.name
  }
}

impl Clone for TableMetadata {
  fn clone(&self) -> Self {
    Self {
      id: self.id,
      name: self.name.clone(),
      filename: self.filename.clone(),
      compaction: self.compaction.clone(),
    }
  }
}

#[cfg(test)]
#[path = "tests/metadata.rs"]
mod test;
