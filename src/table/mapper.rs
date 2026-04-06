use std::{
  collections::HashMap,
  fs::exists,
  path::{Path, PathBuf},
  sync::{
    atomic::{AtomicU16, Ordering},
    Arc,
  },
};

use super::{TableHandle, TableId, TableMetadata};
use crate::{
  disk::{PagePool, PAGE_SIZE},
  metrics::MetricsRegistry,
  utils::{SpinRwLock, ToArc},
  Error, Result,
};

const FILE_SUFFIX: &str = ".db";

enum Slot {
  Reserved,
  Occupied(Arc<TableHandle>),
}

pub enum ReserveResult {
  Found(Arc<TableHandle>),
  New,
  Reserved,
}

pub const META_TABLE: &str = "__meta";
pub const META_TABLE_ID: TableId = 0;

fn to_path(base: &Path, name: &str) -> PathBuf {
  base.join(format!("{name}{FILE_SUFFIX}"))
}

pub struct TableConfig {
  pub base_path: PathBuf,
}

pub struct TableMapper {
  mapping: SpinRwLock<HashMap<String, Slot>>,
  base_path: PathBuf,
  metadata: Arc<TableHandle>,
  page_pool: Arc<PagePool<PAGE_SIZE>>,
  metrics: Arc<MetricsRegistry>,
  last_table_id: AtomicU16,
  is_new: bool,
}
impl TableMapper {
  pub fn new(
    config: TableConfig,
    page_pool: Arc<PagePool<PAGE_SIZE>>,
    metrics: Arc<MetricsRegistry>,
  ) -> Result<Self> {
    let path = to_path(&config.base_path, META_TABLE);
    let is_new = !exists(&path).map_err(Error::IO)?;
    let metadata = TableHandle::open(
      TableMetadata::new(META_TABLE_ID, path),
      page_pool.clone(),
      metrics.clone(),
    )?
    .to_arc();

    Ok(Self {
      mapping: Default::default(),
      base_path: config.base_path,
      metadata,
      page_pool,
      metrics,
      last_table_id: AtomicU16::new(META_TABLE_ID + 1),
      is_new,
    })
  }

  pub fn open_table(&self, table_meta: TableMetadata) -> Result<Arc<TableHandle>> {
    Ok(
      TableHandle::open(table_meta, self.page_pool.clone(), self.metrics.clone())?
        .to_arc(),
    )
  }

  pub fn replay(&self, last_table_id: TableId) {
    self.last_table_id.store(last_table_id, Ordering::Relaxed)
  }

  pub fn is_new(&self) -> bool {
    self.is_new
  }

  pub fn get(&self, name: &str) -> Option<Arc<TableHandle>> {
    match self.mapping.read().get(name)? {
      Slot::Reserved => None,
      Slot::Occupied(i) => Some(i.clone()),
    }
  }
  pub fn get_or_reserve(&self, name: &str) -> Result<ReserveResult> {
    {
      let mut mapping = self.mapping.write();
      if let Some(slot) = mapping.get(name) {
        match slot {
          Slot::Reserved => return Ok(ReserveResult::Reserved),
          Slot::Occupied(i) => return Ok(ReserveResult::Found(i.clone())),
        }
      }
      mapping.insert(name.to_string(), Slot::Reserved);
    }

    Ok(ReserveResult::New)
  }

  pub fn insert(&self, name: String, handle: Arc<TableHandle>) {
    self.mapping.write().insert(name, Slot::Occupied(handle));
  }
  pub fn remove(&self, name: &str) {
    self.mapping.write().remove(name);
  }

  pub fn create_metadata(&self, name: &str) -> TableMetadata {
    TableMetadata::new(
      self.last_table_id.fetch_add(1, Ordering::Relaxed),
      to_path(&self.base_path, name),
    )
  }

  pub fn meta_table(&self) -> Arc<TableHandle> {
    self.metadata.clone()
  }

  pub fn get_all(&self) -> Vec<(String, Arc<TableHandle>)> {
    self
      .mapping
      .read()
      .iter()
      .filter_map(|(k, v)| match v {
        Slot::Reserved => None,
        Slot::Occupied(i) => Some((k.clone(), i.clone())),
      })
      .chain([(META_TABLE.to_string(), self.metadata.clone())])
      .collect()
  }

  pub fn close(&self) {
    for slot in self.mapping.read().values() {
      match slot {
        Slot::Reserved => continue,
        Slot::Occupied(handle) => handle.disk().close(),
      }
    }
    self.metadata.disk().close();
  }
}
