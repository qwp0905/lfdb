use std::ops::Bound;

use super::{BTreeIndex, MergeSortable, ReadonlyPolicy, WritablePolicy};
use crate::{
  cache::BlockCache,
  disk::Pointer,
  table::{PinnedHandle, TableHandleRef, TableMapper, TableMetadata},
  transaction::{PageRecorder, VersionVisibility},
  wal::{TxId, RESERVED_TX},
  Result,
};

struct TableOpenPolicy<'a, R> {
  block_cache: &'a BlockCache,
  version_visibility: &'a VersionVisibility,
  recorder: R,
}
impl<'a, R> ReadonlyPolicy for TableOpenPolicy<'a, R> {
  fn is_aborted(&self, owner: TxId) -> bool {
    self.version_visibility.is_aborted(&owner)
  }
  fn is_owned(&self, _: TxId) -> bool {
    false
  }
  fn is_readable(&self, _: TxId) -> bool {
    true
  }
  fn is_active(&self, _: TxId) -> bool {
    false
  }

  fn fetch_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.block_cache.read(pointer, table)
  }
}

impl<'a> WritablePolicy for TableOpenPolicy<'a, &'a PageRecorder> {
  fn serialize_and_log<T: crate::serialize::Serializable>(
    &self,
    slot: &mut crate::cache::RefedSlot,
    data: &T,
    table: &TableHandleRef,
  ) -> Result {
    self
      .recorder
      .serialize_and_log(RESERVED_TX, table.get_id(), RESERVED_TX, slot, data)
  }

  fn alloc_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.block_cache.alloc(pointer, table)
  }
}

pub fn initialize(
  block_cache: &BlockCache,
  tables: &TableMapper,
  recorder: &PageRecorder,
  version_visibility: &VersionVisibility,
) -> Result {
  let policy = TableOpenPolicy {
    block_cache,
    version_visibility,
    recorder,
  };
  BTreeIndex::new(policy).initialize(&tables.meta_table())?;
  Ok(())
}

pub fn open_tables(
  block_cache: &BlockCache,
  tables: &TableMapper,
  version_visibility: &VersionVisibility,
) -> Result<(
  Vec<(TableHandleRef, TableMetadata)>,
  Vec<((PinnedHandle, TableMetadata), (PinnedHandle, TableMetadata))>,
)> {
  let mut handles = vec![];
  let mut compactions = vec![];
  let meta_table = tables.meta_table();

  let index = BTreeIndex::new(TableOpenPolicy {
    block_cache,
    version_visibility,
    recorder: (),
  });

  let mut iter = index.scan(&meta_table, &Bound::Unbounded, &Bound::Unbounded)?;

  while let Some((_, bytes)) = iter.get_next_pair()? {
    let metadata = TableMetadata::from_bytes(&bytes)?;
    match metadata.get_compaction_metadata() {
      Some(c_meta) => compactions.push((
        (
          tables.create_handle(&metadata)?.try_pin().unwrap(),
          metadata,
        ),
        (tables.create_handle(&c_meta)?.try_pin().unwrap(), c_meta),
      )),
      None => handles.push((tables.create_handle(&metadata)?, metadata)),
    }
  }

  Ok((handles, compactions))
}
