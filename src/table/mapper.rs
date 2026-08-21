use std::{
  collections::HashSet,
  path::PathBuf,
  sync::{atomic::Ordering, Arc, RwLock},
};

use super::{
  AtomicTableId, InitMetadata, TableHandle, TableHandleRef, TableId, TableMetadata,
  TableName, META_TABLE,
};
use crate::{
  cache::ShrinkMap,
  disk::{BlockIOHandle, IOPool},
  utils::{uuid_simple, SBox, ShortenedRwLock},
  Result,
};

const FILE_EXT: &str = "db";

pub const META_TABLE_ID: TableId = 0;

fn to_path(table_name: &TableName) -> PathBuf {
  // Keep the logical table name in the filename for readability, but use a UUID
  // for uniqueness because one logical table may produce multiple backing files.
  PathBuf::from(format!("{table_name}_{}", uuid_simple())).with_extension(FILE_EXT)
}

/**
 * Runtime registry for opened table segments.
 *
 * Historically this type mapped table names to table handles, hence the name.
 * That responsibility has moved elsewhere; today it mainly owns the metadata
 * table handle, tracks opened table handles by id, allocates new table metadata,
 * and reconciles table files during replay.
 */
pub struct TableMapper {
  open_handles: RwLock<ShrinkMap<TableId, TableHandleRef>>,
  metadata: TableHandleRef,
  io_pool: Arc<IOPool>,
  last_table_id: AtomicTableId,
}
impl TableMapper {
  pub fn open_new(io_pool: Arc<IOPool>) -> Result<(Self, InitMetadata)> {
    let filename = format!("{}.{}", META_TABLE, FILE_EXT);
    let init = InitMetadata::new(META_TABLE_ID, META_TABLE.to_string(), filename.clone());
    let disk = BlockIOHandle::new(io_pool.open_direct_io(PathBuf::from(filename))?);
    let metadata = TableHandle::new(&init.try_cast().unwrap(), disk);
    Ok((
      Self {
        open_handles: Default::default(),
        metadata: SBox::new(metadata),
        io_pool,
        last_table_id: AtomicTableId::new(META_TABLE_ID + 1),
      },
      init,
    ))
  }
  pub fn open_exists(io_pool: Arc<IOPool>, init: &InitMetadata) -> Result<Self> {
    let casted = init.try_cast()?;
    let disk =
      BlockIOHandle::new(io_pool.open_direct_io(casted.get_filename().to_path_buf())?);
    let metadata = TableHandle::new(&casted, disk);
    Ok(Self {
      open_handles: Default::default(),
      metadata: SBox::new(metadata),
      io_pool,
      last_table_id: AtomicTableId::new(META_TABLE_ID + 1),
    })
  }

  pub fn create_handle(&self, table_meta: &TableMetadata) -> Result<TableHandleRef> {
    let io = self
      .io_pool
      .open_direct_io(table_meta.get_filename().to_path_buf())?;
    let handle = BlockIOHandle::new(io);
    Ok(SBox::new(TableHandle::new(table_meta, handle)))
  }

  /**
   * Rebuild the open table registry from replay results.
   *
   * The caller decides which table metadata entries are live and passes their
   * opened handles here. `TableMapper` simply registers every supplied handle,
   * advances the next table id, and removes `.db` files in the base directory that
   * were not supplied by the replay result.
   */
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

    for (metadata, table) in iter {
      exists.remove(metadata.get_filename());
      let id = metadata.get_id();
      self.last_table_id.fetch_max(id + 1, Ordering::Relaxed);
      self.open_handles.wl().insert(id, table);
    }

    for filename in exists {
      self.io_pool.truncate(&filename)?;
    }
    Ok(())
  }

  pub fn get(&self, id: TableId) -> Option<TableHandleRef> {
    self.open_handles.rl().get(&id).cloned()
  }

  pub fn insert(&self, handle: TableHandleRef) {
    self.open_handles.wl().insert(handle.get_id(), handle);
  }
  pub fn remove(&self, id: TableId) {
    self.open_handles.wl().remove(&id);
  }

  /**
   * Allocate a fresh table metadata record.
   *
   * This atomically reserves the next table id and pairs it with a newly generated
   * backing filename. Persisting the metadata is the caller's responsibility.
   */
  pub fn create_metadata(&self, name: &TableName) -> TableMetadata {
    let id = self.last_table_id.fetch_add(1, Ordering::Relaxed);
    TableMetadata::new(id, name.clone(), to_path(name))
  }

  pub fn meta_table(&self) -> TableHandleRef {
    self.metadata.clone()
  }
  pub fn meta_table_id(&self) -> TableId {
    self.metadata.get_id()
  }

  /**
   * Return every table handle known to the registry, including the metadata table.
   */
  pub fn get_all(&self) -> Vec<TableHandleRef> {
    self
      .open_handles
      .rl()
      .values()
      .cloned()
      .chain([self.metadata.clone()])
      .collect()
  }
}
