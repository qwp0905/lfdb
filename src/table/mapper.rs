use std::{
  collections::HashMap,
  fs::exists,
  mem::replace,
  path::{Path, PathBuf},
  sync::{
    atomic::{AtomicU16, Ordering},
    Arc,
  },
};

use super::{PinnedHandle, TableHandle, TableId, TableMetadata};
use crate::{
  disk::{PagePool, PAGE_SIZE},
  metrics::MetricsRegistry,
  thread::TaskHandle,
  utils::{SpinRwLock, ToArc},
  Error, Result,
};

const FILE_SUFFIX: &str = ".db";

enum Slot {
  Reserved,
  PendingDrop(Arc<TableHandle>),
  InDrop(TableId, TaskHandle<()>),
  Occupied(Arc<TableHandle>),
}

pub enum ReserveResult {
  Found(PinnedHandle),
  New,
  Reserved,
}
pub enum DropResult {
  Reserved(Arc<TableHandle>),
  NotFound,
  Conflict,
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

  pub fn create_handle(&self, table_meta: TableMetadata) -> Result<Arc<TableHandle>> {
    Ok(
      TableHandle::open(table_meta, self.page_pool.clone(), self.metrics.clone())?
        .to_arc(),
    )
  }

  pub fn replay<Iter: Iterator<Item = (String, Arc<TableHandle>)>>(
    &self,
    iter: Iter,
  ) -> Result {
    self.metadata.replay()?;
    for (name, table) in iter {
      table.replay()?;
      self
        .last_table_id
        .fetch_max(table.metadata().get_id() + 1, Ordering::Relaxed);
      self.mapping.write().insert(name, Slot::Occupied(table));
    }
    Ok(())
  }

  pub fn is_new(&self) -> bool {
    self.is_new
  }

  pub fn get(&self, name: &str) -> Option<PinnedHandle> {
    match self.mapping.read().get(name)? {
      Slot::Reserved => None,
      Slot::InDrop(_, _) => None,
      Slot::Occupied(i) => Some(PinnedHandle::new(i.clone())),
      Slot::PendingDrop(i) => Some(PinnedHandle::new(i.clone())),
    }
  }
  pub fn get_or_reserve(&self, name: &str) -> Result<ReserveResult> {
    let mut mapping = self.mapping.write();
    if let Some(slot) = mapping.get_mut(name) {
      match slot {
        Slot::Reserved => return Ok(ReserveResult::Reserved),
        Slot::InDrop(_, _) => {
          if let Slot::InDrop(_, o) = replace(slot, Slot::Reserved) {
            drop(mapping);
            let _ = o.wait();
            return Ok(ReserveResult::New);
          }
        }
        Slot::Occupied(i) => {
          return Ok(ReserveResult::Found(PinnedHandle::new(i.clone())))
        }
        Slot::PendingDrop(i) => {
          return Ok(ReserveResult::Found(PinnedHandle::new(i.clone())))
        }
      }
    }
    mapping.insert(name.to_string(), Slot::Reserved);
    Ok(ReserveResult::New)
  }

  pub fn try_drop(&self, name: &str) -> DropResult {
    if let Some(slot) = self.mapping.write().get_mut(name) {
      match slot {
        Slot::Occupied(handle) => {
          let cloned = handle.clone();
          *slot = Slot::PendingDrop(cloned.clone());
          return DropResult::Reserved(cloned);
        }
        Slot::PendingDrop(_) => return DropResult::Conflict,
        _ => {}
      }
    };
    DropResult::NotFound
  }

  pub fn insert(&self, name: String, handle: Arc<TableHandle>) {
    self.mapping.write().insert(name, Slot::Occupied(handle));
  }
  pub fn remove(&self, id: TableId, name: &str) {
    let mut mapping = self.mapping.write();
    if let Some(Slot::InDrop(i, _)) = mapping.get(name) {
      if *i == id {
        mapping.remove(name);
      }
    }
  }
  pub fn drop_reserve(&self, id: TableId, name: &str, handle: TaskHandle<()>) {
    self
      .mapping
      .write()
      .insert(name.to_string(), Slot::InDrop(id, handle));
  }

  pub fn create_metadata(&self, name: &str) -> TableMetadata {
    TableMetadata::new(
      self.last_table_id.fetch_add(1, Ordering::Relaxed),
      to_path(&self.base_path, name),
    )
  }

  pub fn meta_table(&self) -> PinnedHandle {
    PinnedHandle::new(self.metadata.clone())
  }

  pub fn get_all(&self) -> Vec<(String, Arc<TableHandle>)> {
    self
      .mapping
      .read()
      .iter()
      .filter_map(|(k, v)| match v {
        Slot::Reserved => None,
        Slot::InDrop(_, _) => None,
        Slot::Occupied(i) => Some((k.clone(), i.clone())),
        Slot::PendingDrop(i) => Some((k.clone(), i.clone())),
      })
      .chain([(META_TABLE.to_string(), self.metadata.clone())])
      .collect()
  }

  pub fn close(&self) {
    for slot in self.mapping.read().values() {
      match slot {
        Slot::Reserved => continue,
        Slot::InDrop(_, _) => continue,
        Slot::Occupied(handle) => handle.disk().close(),
        Slot::PendingDrop(handle) => handle.disk().close(),
      }
    }
    self.metadata.disk().close();
  }
}
