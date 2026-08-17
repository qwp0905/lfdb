use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{disk::IOPool, table::InitMetadata, Error, Result};

const FILENAME: &str = "manifest.toml";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
  pub page_size: u32,
  pub metadata_table: InitMetadata,
  pub snapshot_version: u16,
  pub wal_version: u16,
}
impl Manifest {
  pub const fn new(
    page_size: u32,
    metadata_table: InitMetadata,
    snapshot_version: u16,
    wal_version: u16,
  ) -> Self {
    Self {
      page_size,
      metadata_table,
      snapshot_version,
      wal_version,
    }
  }
}

pub fn load_manifest(io_pool: &IOPool) -> Result<Option<Manifest>> {
  let filename = PathBuf::from(FILENAME);
  if !io_pool.exists(&filename)? {
    return Ok(None);
  };
  let mut file = io_pool.open_scan_io(filename)?;
  let bytes = file.read_to_vec(file.len() as usize)?;
  match toml::from_slice::<Manifest>(&bytes) {
    Ok(manifest) => Ok(Some(manifest)),
    Err(_) => Err(Error::InvalidFormat("manifest crashed.")),
  }
}

pub fn save_manifest(io_pool: &IOPool, manifest: &Manifest) -> Result {
  let Ok(str) = toml::to_string(manifest) else {
    return Err(Error::InvalidFormat("manifest cannot be serialized."));
  };

  let mut file = io_pool.open_append_io(PathBuf::from(FILENAME))?;
  file.append(str.as_bytes())?;
  while !file.is_aligned() {
    file.append(" ".as_bytes())?;
  }
  file.flush_all()?;
  Ok(())
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use tempfile::TempDir;

  use crate::{metrics::MetricsRegistry, DefaultIOBackend};

  use super::*;

  #[test]
  fn test_save_and_load() {
    let dir = TempDir::new_in(".").expect("dir failed.");
    let metrics = Arc::new(MetricsRegistry::new());
    let io_pool = IOPool::with_backend(DefaultIOBackend, 1, dir.path(), metrics).unwrap();

    let page_size = 123;
    let tid = 1;
    let name = "name".to_string();
    let filename = "filename".to_string();
    let metadata = InitMetadata::new(tid, name.clone(), filename.clone());
    let sversion = 10;
    let wversion = 199;
    let manifest = Manifest::new(page_size, metadata, sversion, wversion);

    save_manifest(&io_pool, &manifest).unwrap();

    let parsed = load_manifest(&io_pool).unwrap().unwrap();

    assert_eq!(parsed.page_size, page_size);
    assert_eq!(parsed.metadata_table.name, name);
    assert_eq!(parsed.metadata_table.filename, filename);
    assert_eq!(parsed.metadata_table.id, tid);

    assert_eq!(parsed.snapshot_version, sversion);
    assert_eq!(parsed.wal_version, wversion);

    io_pool.close();
  }
}
