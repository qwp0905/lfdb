use std::{
  collections::{HashMap, HashSet},
  path::PathBuf,
  sync::{atomic::Ordering, Arc, RwLock},
};

use super::{
  AtomicTableId, TableHandle, TableHandleRef, TableId, TableMetadata, TableName,
  META_TABLE,
};
use crate::{
  disk::{DiskController, IOPool},
  utils::{uuid_simple, SBox, ShortenedRwLock},
  Result,
};

const FILE_EXT: &str = "db";

pub const META_TABLE_ID: TableId = 0;

fn to_path(table_name: &TableName) -> PathBuf {
  PathBuf::from(format!("{table_name}_{}", uuid_simple())).with_extension(FILE_EXT)
}

pub struct TableMapper {
  open_handles: RwLock<HashMap<TableId, TableHandleRef>>,
  metadata: TableHandleRef,
  io_pool: Arc<IOPool>,
  last_table_id: AtomicTableId,
  is_new: bool,
}
impl TableMapper {
  pub fn new(io_pool: Arc<IOPool>) -> Result<Self> {
    let filename = PathBuf::from(META_TABLE).with_extension(FILE_EXT);
    let is_new = !io_pool.exists(&filename)?;

    let disk = DiskController::new(io_pool.create_handle(filename.clone())?);
    let metadata = TableHandle::new(
      &TableMetadata::new(
        META_TABLE_ID,
        TableName::from_str_unchecked(META_TABLE),
        filename,
      ),
      disk,
    );

    Ok(Self {
      open_handles: Default::default(),
      metadata: SBox::new(metadata),
      io_pool,
      last_table_id: AtomicTableId::new(META_TABLE_ID + 1),
      is_new,
    })
  }

  pub fn create_handle(&self, table_meta: &TableMetadata) -> Result<TableHandleRef> {
    let disk = DiskController::new(
      self
        .io_pool
        .create_handle(table_meta.get_filename().to_path_buf())?,
    );
    Ok(SBox::new(TableHandle::new(table_meta, disk)))
  }

  pub fn replay<Iter: Iterator<Item = (TableMetadata, TableHandleRef)>>(
    &self,
    iter: Iter,
  ) -> Result {
    let mut exists = HashSet::new();
    for entry in self.io_pool.read_dir()? {
      let filename = PathBuf::from(entry.file_name());
      if filename.extension().is_none_or(|ext| ext != FILE_EXT) {
        continue;
      }
      exists.insert(filename);
    }

    self.metadata.replay()?;

    for (metadata, table) in iter {
      exists.remove(metadata.get_filename());
      table.replay()?;
      let id = metadata.get_id();
      self.last_table_id.fetch_max(id + 1, Ordering::Relaxed);
      self.open_handles.wl().insert(id, table);
    }

    for filename in exists {
      self.io_pool.remove(&filename)?;
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
    self.open_handles.wl().insert(handle.get_id(), handle);
  }
  pub fn remove(&self, id: TableId) {
    self.open_handles.wl().remove(&id);
  }

  pub fn create_metadata(&self, name: &TableName) -> TableMetadata {
    let id = self.last_table_id.fetch_add(1, Ordering::Relaxed);
    TableMetadata::new(id, name.clone(), to_path(name))
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
}
