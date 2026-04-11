use std::{
  collections::HashMap,
  fs::exists,
  path::{Path, PathBuf},
  sync::{atomic::Ordering, Arc},
};

use super::{AtomicTableId, TableHandle, TableId, TableMetadata};
use crate::{
  disk::{PagePool, PAGE_SIZE},
  metrics::MetricsRegistry,
  utils::{SpinRwLock, ToArc},
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
}

pub struct TableMapper {
  open_handles: SpinRwLock<HashMap<TableId, Arc<TableHandle>>>,
  base_path: PathBuf,
  metadata: Arc<TableHandle>,
  page_pool: Arc<PagePool<PAGE_SIZE>>,
  metrics: Arc<MetricsRegistry>,
  last_table_id: AtomicTableId,
  is_new: bool,
}
impl TableMapper {
  pub fn new(
    config: TableConfig,
    page_pool: Arc<PagePool<PAGE_SIZE>>,
    metrics: Arc<MetricsRegistry>,
  ) -> Result<Self> {
    let path = to_path(&config.base_path, META_TABLE_ID);
    let is_new = !exists(&path).map_err(Error::IO)?;
    let metadata = TableHandle::open(
      TableMetadata::new(META_TABLE_ID, META_TABLE.to_string(), path),
      page_pool.clone(),
      metrics.clone(),
    )?
    .to_arc();

    Ok(Self {
      open_handles: Default::default(),
      base_path: config.base_path,
      metadata,
      page_pool,
      metrics,
      last_table_id: AtomicTableId::new(META_TABLE_ID + 1),
      is_new,
    })
  }

  pub fn create_handle(&self, table_meta: TableMetadata) -> Result<Arc<TableHandle>> {
    Ok(
      TableHandle::open(table_meta, self.page_pool.clone(), self.metrics.clone())?
        .to_arc(),
    )
  }

  pub fn replay<Iter: Iterator<Item = Arc<TableHandle>>>(&self, iter: Iter) -> Result {
    self.metadata.replay()?;

    for table in iter {
      table.replay()?;
      let id = table.metadata().get_id();
      self.last_table_id.fetch_max(id + 1, Ordering::Relaxed);
      self.open_handles.write().insert(id, table);
    }
    Ok(())
  }

  pub fn is_new(&self) -> bool {
    self.is_new
  }

  pub fn get(&self, id: TableId) -> Option<Arc<TableHandle>> {
    self
      .open_handles
      .read()
      .get(&id)
      .map(|handle| handle.clone())
  }

  pub fn insert(&self, handle: Arc<TableHandle>) {
    self
      .open_handles
      .write()
      .insert(handle.metadata().get_id(), handle);
  }
  pub fn remove(&self, id: TableId) {
    self.open_handles.write().remove(&id);
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
      .read()
      .values()
      .map(|v| v.clone())
      .chain([self.metadata.clone()])
      .collect()
  }

  pub fn close(&self) {
    for handle in self.open_handles.read().values() {
      handle.disk().close();
    }
    self.metadata.disk().close();
  }
}
