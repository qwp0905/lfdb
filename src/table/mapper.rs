use std::{
  collections::{HashMap, HashSet},
  fs::{exists, read_dir, remove_file},
  path::{Path, PathBuf},
  sync::{atomic::Ordering, Arc, RwLock},
};

use super::{AtomicTableId, TableHandle, TableHandleRef, TableId, TableMetadata};
use crate::{
  disk::{DiskController, IOPool},
  utils::{ShortenedRwLock, ToArc},
  Error, Result,
};

const FILE_EXT: &str = "db";

pub const META_TABLE: &str = "__meta";
pub const META_TABLE_ID: TableId = 0;

fn to_path(base: &Path, id: TableId) -> PathBuf {
  base.join(format!("{id}")).with_extension(FILE_EXT)
}

pub struct TableConfig {
  pub base_path: PathBuf,
}

pub struct TableMapper {
  open_handles: RwLock<HashMap<TableId, TableHandleRef>>,
  base_path: PathBuf,
  metadata: TableHandleRef,
  io_pool: Arc<IOPool>,
  last_table_id: AtomicTableId,
  is_new: bool,
}
impl TableMapper {
  pub fn new(config: TableConfig, io_pool: Arc<IOPool>) -> Result<Self> {
    let path = to_path(&config.base_path, META_TABLE_ID);
    let is_new = !exists(&path).map_err(Error::IO)?;

    let disk = DiskController::new(io_pool.create_handle(&path)?);
    let metadata = TableHandle::new(
      &TableMetadata::new(META_TABLE_ID, META_TABLE.to_string(), path),
      disk,
    )
    .to_arc();

    Ok(Self {
      open_handles: Default::default(),
      base_path: config.base_path,
      metadata,
      io_pool,
      last_table_id: AtomicTableId::new(META_TABLE_ID + 1),
      is_new,
    })
  }

  pub fn create_handle(&self, table_meta: &TableMetadata) -> Result<TableHandleRef> {
    let disk = DiskController::new(self.io_pool.create_handle(table_meta.get_path())?);
    Ok(TableHandle::new(table_meta, disk).to_arc())
  }

  pub fn replay<Iter: Iterator<Item = TableHandleRef>>(&self, iter: Iter) -> Result {
    let dir = read_dir(&self.base_path).map_err(Error::IO)?;
    let mut exists = HashSet::new();
    for entry in dir {
      let path = entry.map_err(Error::IO)?.path();
      if path.extension().is_none_or(|ext| ext != FILE_EXT) {
        continue;
      }
      if path == self.metadata.metadata().get_path() {
        continue;
      }
      exists.insert(path);
    }

    self.metadata.replay()?;

    for table in iter {
      exists.remove(table.metadata().get_path());
      table.replay()?;
      let id = table.metadata().get_id();
      self.last_table_id.fetch_max(id + 1, Ordering::Relaxed);
      self.open_handles.wl().insert(id, table);
    }

    for path in exists {
      remove_file(path).map_err(Error::IO)?;
    }
    Ok(())
  }

  pub fn is_new(&self) -> bool {
    self.is_new
  }

  pub fn get(&self, id: TableId) -> Option<TableHandleRef> {
    self.open_handles.rl().get(&id).map(|handle| handle.clone())
  }

  pub fn insert(&self, handle: TableHandleRef) {
    self
      .open_handles
      .wl()
      .insert(handle.metadata().get_id(), handle);
  }
  pub fn remove(&self, id: TableId) {
    self.open_handles.wl().remove(&id);
  }

  pub fn create_metadata(&self, str: &str) -> TableMetadata {
    let id = self.last_table_id.fetch_add(1, Ordering::Relaxed);
    TableMetadata::new(id, str.to_string(), to_path(&self.base_path, id))
  }

  pub fn meta_table(&self) -> TableHandleRef {
    self.metadata.clone()
  }

  pub fn get_all(&self) -> Vec<TableHandleRef> {
    self
      .open_handles
      .rl()
      .values()
      .map(|v| v.clone())
      .chain([self.metadata.clone()])
      .collect()
  }

  pub fn close(&self) {
    self.io_pool.close();
  }
}
