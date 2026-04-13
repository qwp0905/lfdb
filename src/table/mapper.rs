use std::{
  collections::{HashMap, HashSet},
  fs::{exists, read_dir, remove_file},
  path::{Path, PathBuf},
  sync::{atomic::Ordering, Arc, RwLock},
};

use super::{AtomicTableId, TableHandle, TableId, TableMetadata};
use crate::{
  disk::{IOPool, PagePool, PAGE_SIZE},
  metrics::MetricsRegistry,
  utils::{ShortenedRwLock, ToArc},
  Error, Result,
};

const FILE_SUFFIX: &str = ".db";

pub const META_TABLE: &str = "__meta";
pub const META_TABLE_ID: TableId = 0;

fn to_path(base: &Path, id: TableId) -> PathBuf {
  base.join(format!("{id}{FILE_SUFFIX}"))
}

pub struct TableConfig {
  pub base_path: PathBuf,
  pub io_thread_count: usize,
}

pub struct TableMapper {
  open_handles: RwLock<HashMap<TableId, Arc<TableHandle>>>,
  base_path: PathBuf,
  metadata: Arc<TableHandle>,
  io_pool: IOPool<PAGE_SIZE>,
  last_table_id: AtomicTableId,
  is_new: bool,
}
impl TableMapper {
  pub fn new(
    config: TableConfig,
    page_pool: Arc<PagePool<PAGE_SIZE>>,
    metrics: Arc<MetricsRegistry>,
  ) -> Result<Self> {
    let io_pool = IOPool::new(config.io_thread_count, page_pool, metrics.clone());

    let path = to_path(&config.base_path, META_TABLE_ID);
    let is_new = !exists(&path).map_err(Error::IO)?;

    let disk = io_pool.open_controller(&path)?;
    let metadata = TableHandle::new(
      TableMetadata::new(META_TABLE_ID, META_TABLE.to_string(), path),
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

  pub fn create_handle(&self, table_meta: TableMetadata) -> Result<Arc<TableHandle>> {
    let disk = self.io_pool.open_controller(table_meta.get_path())?;
    Ok(TableHandle::new(table_meta, disk).to_arc())
  }

  pub fn replay<Iter: Iterator<Item = Arc<TableHandle>>>(&self, iter: Iter) -> Result {
    let dir = read_dir(&self.base_path).map_err(Error::IO)?;
    let mut exists = HashSet::new();
    for entry in dir {
      let entry = entry.map_err(Error::IO)?;
      if !entry.file_name().to_string_lossy().ends_with(FILE_SUFFIX) {
        continue;
      }
      if entry.path() == self.metadata.metadata().get_path() {
        continue;
      }
      exists.insert(entry.path());
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

  pub fn get(&self, id: TableId) -> Option<Arc<TableHandle>> {
    self.open_handles.rl().get(&id).map(|handle| handle.clone())
  }

  pub fn insert(&self, handle: Arc<TableHandle>) {
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

  pub fn meta_table(&self) -> Arc<TableHandle> {
    self.metadata.clone()
  }

  pub fn get_all(&self) -> Vec<Arc<TableHandle>> {
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
