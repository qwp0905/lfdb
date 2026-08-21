use std::{
  ffi::OsStr,
  fmt,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFormatVersion {
  V0,
}
impl TableFormatVersion {
  pub const CURRENT: Self = Self::V0;

  pub const fn from_u16(version: u16) -> Option<Self> {
    match version {
      0 => Some(Self::V0),
      _ => None,
    }
  }
  pub const fn as_u16(&self) -> u16 {
    match self {
      Self::V0 => 0,
    }
  }
  pub const fn is_current(&self) -> bool {
    matches!(self, &Self::CURRENT)
  }
  const fn as_str(&self) -> &str {
    match self {
      Self::V0 => "v0",
    }
  }

  const BYTE_LEN: usize = u16::BITS as usize >> 3;
}

impl fmt::Display for TableFormatVersion {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> fmt::Result {
    fmt::Display::fmt(self.as_str(), f)
  }
}

#[derive(Debug)]
struct SegmentSpec {
  id: TableId,
  filename: PathBuf,
  version: TableFormatVersion,
}
impl SegmentSpec {
  const fn new(id: TableId, filename: PathBuf, version: TableFormatVersion) -> Self {
    Self {
      id,
      filename,
      version,
    }
  }
  fn read_from(reader: &mut OffsetReader) -> Result<Self> {
    let Some(id) = reader.read_array().map(TableId::from_le_bytes) else {
      return Err(Error::InvalidFormat("metadata crashed."));
    };
    let Some(v) = reader.read_u16() else {
      return Err(Error::InvalidFormat("metadata crashed."));
    };
    let Some(version) = TableFormatVersion::from_u16(v) else {
      return Err(Error::UnsupportedVersion);
    };
    let Some(len) = reader.read_u16() else {
      return Err(Error::InvalidFormat("metadata crashed."));
    };
    let Some(bytes) = reader.read(len as usize) else {
      return Err(Error::InvalidFormat("metadata crashed."));
    };
    let filename = unsafe { OsStr::from_encoded_bytes_unchecked(bytes) };
    Ok(Self {
      id,
      filename: PathBuf::from(filename),
      version,
    })
  }
  fn write_to(&self, writer: &mut OffsetWriter) {
    writer.write(&self.id.to_le_bytes());
    writer.write_u16(self.version.as_u16());

    let bytes = self.filename.as_os_str().as_encoded_bytes();
    writer.write_u16(bytes.len() as u16);
    writer.write(bytes);
  }

  fn byte_len(&self) -> usize {
    TABLE_ID_BYTES
      + 2
      + self.filename.as_os_str().as_encoded_bytes().len()
      + TableFormatVersion::BYTE_LEN
  }
}
impl Clone for SegmentSpec {
  fn clone(&self) -> Self {
    Self {
      id: self.id,
      filename: self.filename.clone(),
      version: self.version,
    }
  }
}

/**
 * Durable table descriptor stored in the metadata table.
 *
 * This value records the table id, validated table name, backing filename, and
 * optional compaction state. `to_vec` and `from_bytes` encode/decode the binary
 * value stored in the metadata table.
 */
#[derive(Debug)]
pub struct TableMetadata {
  name: TableName,
  spec: SegmentSpec,
  /**
   * In-progress compaction target, if any.
   *
   * The primary role is to elect a single compaction winner for a table by making
   * the in-progress target visible in metadata. Because the marker is serialized
   * with the table descriptor, recovery can also discover the compaction target
   * after a crash.
   */
  compaction: Option<SegmentSpec>,
}
impl TableMetadata {
  pub const fn new(id: TableId, name: TableName, filename: PathBuf) -> Self {
    Self {
      name,
      spec: SegmentSpec::new(id, filename, TableFormatVersion::CURRENT),
      compaction: None,
    }
  }

  pub fn set_compaction(&mut self, metadata: &TableMetadata) {
    self.compaction = Some(metadata.spec.clone());
  }

  pub const fn get_compaction_id(&self) -> Option<TableId> {
    match &self.compaction {
      Some(spec) => Some(spec.id),
      None => None,
    }
  }
  /**
   * Return metadata for the table produced by compaction.
   *
   * Compaction keeps the same logical table name but writes into a different id
   * and backing file.
   */
  pub fn get_compaction_metadata(&self) -> Option<TableMetadata> {
    let spec = self.compaction.as_ref()?;
    Some(Self {
      name: self.name.clone(),
      spec: spec.clone(),
      compaction: None,
    })
  }

  pub fn byte_len(&self) -> usize {
    1 + self
      .compaction
      .as_ref()
      .map(|spec| spec.byte_len())
      .unwrap_or(0)
      + self.spec.byte_len()
      + 2
      + self.name.len()
  }

  pub fn to_vec(&self) -> Vec<u8> {
    let mut bytes = vec![0; self.byte_len()];
    let mut writer = OffsetWriter::new(&mut bytes);

    match self.compaction.as_ref() {
      Some(spec) => {
        writer.write_u8(1);
        spec.write_to(&mut writer);
      }
      None => {
        writer.write_u8(0);
      }
    }

    writer.write_u16(self.name.len() as u16);
    writer.write(self.name.as_bytes());

    self.spec.write_to(&mut writer);
    debug_assert_eq!(writer.written_bytes(), bytes.len());
    bytes
  }

  pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
    let mut reader = OffsetReader::new(bytes);
    let compaction = match reader.read_byte() {
      Some(0) => None,
      Some(1) => Some(SegmentSpec::read_from(&mut reader)?),
      _ => return Err(Error::InvalidFormat("metadata crashed.")),
    };

    let Some(name_len) = reader.read_u16() else {
      return Err(Error::InvalidFormat("metadata crashed."));
    };
    let Some(name) = reader.read(name_len as usize) else {
      return Err(Error::InvalidFormat("metadata crashed."));
    };
    let name = unsafe { TableName::from_str_unchecked(str::from_utf8_unchecked(name)) };
    let spec = SegmentSpec::read_from(&mut reader)?;
    Ok(Self {
      name,
      spec,
      compaction,
    })
  }

  #[inline]
  pub const fn get_id(&self) -> TableId {
    self.spec.id
  }
  #[inline]
  pub fn get_filename(&self) -> &Path {
    &self.spec.filename
  }
  #[inline]
  pub const fn get_name(&self) -> &TableName {
    &self.name
  }
  pub const fn get_version(&self) -> TableFormatVersion {
    self.spec.version
  }
}

impl Clone for TableMetadata {
  fn clone(&self) -> Self {
    Self {
      name: self.name.clone(),
      spec: self.spec.clone(),
      compaction: self.compaction.as_ref().cloned(),
    }
  }
}

#[cfg(test)]
#[path = "tests/metadata.rs"]
mod test;
