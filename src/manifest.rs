use std::path::PathBuf;

use crate::{
  disk::IOPool, table::TableMetadata, transaction::SnapshotFormatVersion,
  utils::OffsetReader, wal::WALFormatVersion, Error, Result,
};

const FILENAME: &str = "manifest";

fn calc_checksum(bytes: &[u8]) -> u32 {
  let mut hasher = crc32fast::Hasher::new();
  hasher.update(bytes);
  hasher.finalize()
}

#[derive(Debug, Clone, Copy)]
enum ManifestVersion {
  V0,
}
impl ManifestVersion {
  const CURRENT: Self = Self::V0;
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

  fn serialize(&self, manifest: &Manifest) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(self.as_u16().to_le_bytes());
    match self {
      Self::V0 => {
        bytes.extend(manifest.page_size.to_le_bytes());

        let metadata = manifest.metadata_table.to_vec();
        bytes.extend(&(metadata.len() as u32).to_le_bytes());
        bytes.extend(metadata);

        bytes.extend(manifest.snapshot_version.as_u16().to_le_bytes());
        bytes.extend(manifest.wal_version.as_u16().to_le_bytes());
      }
    };
    bytes
  }

  fn deserialize(&self, reader: &mut OffsetReader) -> Result<Manifest> {
    match self {
      Self::V0 => {
        let Some(page_size) = reader.read_u32() else {
          return Err(CRASHED_ERR);
        };

        let Some(len) = reader.read_u32() else {
          return Err(CRASHED_ERR);
        };
        let Some(bytes) = reader.read(len as usize) else {
          return Err(CRASHED_ERR);
        };
        let metadata_table = TableMetadata::from_bytes(bytes)?;

        let Some(v) = reader.read_u16() else {
          return Err(CRASHED_ERR);
        };
        let Some(snapshot_version) = SnapshotFormatVersion::from_u16(v) else {
          return Err(Error::UnsupportedVersion);
        };

        let Some(v) = reader.read_u16() else {
          return Err(CRASHED_ERR);
        };
        let Some(wal_version) = WALFormatVersion::from_u16(v) else {
          return Err(Error::UnsupportedVersion);
        };
        Ok(Manifest {
          page_size,
          metadata_table,
          snapshot_version,
          wal_version,
        })
      }
    }
  }
}

pub struct Manifest {
  pub page_size: u32,
  pub metadata_table: TableMetadata,
  pub snapshot_version: SnapshotFormatVersion,
  pub wal_version: WALFormatVersion,
}
impl Manifest {
  pub const fn new(
    page_size: u32,
    metadata_table: TableMetadata,
    snapshot_version: SnapshotFormatVersion,
    wal_version: WALFormatVersion,
  ) -> Self {
    Self {
      page_size,
      metadata_table,
      snapshot_version,
      wal_version,
    }
  }
}
const CRASHED_ERR: Error = Error::InvalidFormat("metadata crashed.");
pub fn load_manifest(io_pool: &IOPool) -> Result<Option<Manifest>> {
  let filename = PathBuf::from(FILENAME);
  if !io_pool.exists(&filename)? {
    return Ok(None);
  };

  let mut file = io_pool.open_scan_io(filename)?;
  let len = u32::from_le_bytes(file.read_array::<4>()?) as usize;
  let bytes = file.read_to_vec(len)?;
  let mut reader = OffsetReader::new(&bytes);
  if reader
    .read_u32()
    .is_none_or(|c| c != calc_checksum(&bytes[4..]))
  {
    return Err(CRASHED_ERR);
  }
  let Some(v) = reader.read_u16() else {
    return Err(CRASHED_ERR);
  };
  let Some(version) = ManifestVersion::from_u16(v) else {
    return Err(Error::UnsupportedVersion);
  };
  version.deserialize(&mut reader).map(Some)
}

pub fn save_manifest(io_pool: &IOPool, manifest: &Manifest) -> Result {
  let bytes = ManifestVersion::CURRENT.serialize(manifest);
  let checksum = calc_checksum(&bytes);

  let mut file = io_pool.open_append_io(PathBuf::from(FILENAME))?;

  file.append(&(bytes.len() as u32 + 4).to_le_bytes())?;
  file.append(&checksum.to_le_bytes())?;
  file.append(&bytes)?;
  file.flush_all()?;
  Ok(())
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use tempfile::TempDir;

  use crate::{metrics::MetricsRegistry, table::TableName, DefaultIOBackend};

  use super::*;

  #[test]
  fn test_save_and_load() {
    let dir = TempDir::new_in(".").expect("dir failed.");
    let metrics = Arc::new(MetricsRegistry::new());
    let io_pool = IOPool::with_backend(DefaultIOBackend, dir.path(), metrics).unwrap();

    let page_size = 123;
    let tid = 1;
    let name = TableName::from_str("name").unwrap();
    let filename = PathBuf::from("filename".to_string());
    let metadata = TableMetadata::new(tid, name.clone(), filename.clone());
    let sversion = SnapshotFormatVersion::CURRENT;
    let wversion = WALFormatVersion::CURRENT;
    let manifest = Manifest::new(page_size, metadata, sversion, wversion);

    save_manifest(&io_pool, &manifest).unwrap();

    let parsed = load_manifest(&io_pool).unwrap().unwrap();

    assert_eq!(parsed.page_size, page_size);
    assert_eq!(parsed.metadata_table.get_name(), &name);
    assert_eq!(parsed.metadata_table.get_filename(), filename);
    assert_eq!(parsed.metadata_table.get_id(), tid);

    assert_eq!(parsed.snapshot_version, sversion);
    assert_eq!(parsed.wal_version, wversion);

    io_pool.close();
  }
}
