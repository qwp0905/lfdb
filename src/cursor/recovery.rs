use std::{collections::HashSet, ops::Bound, sync::Arc};

use crossbeam::queue::SegQueue;

use super::{
  BTreeIndex, BTreeNodeView, BlobStorage, DataEntryView, MergeSortable, ReadonlyPolicy,
  TreeHeader, WritablePolicy, HEADER_POINTER,
};
use crate::{
  background::once,
  cache::BlockCache,
  debug,
  disk::{AlignedBuf, Pointer},
  info,
  table::{PinnedHandle, TableHandleRef, TableMapper, TableMetadata},
  transaction::{PageRecorder, VersionVisibility},
  wal::{TxId, RESERVED_TX},
  Result,
};

struct TableOpenPolicy<'a, R> {
  block_cache: &'a BlockCache,
  version_visibility: &'a VersionVisibility,
  blob: &'a BlobStorage,
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
  fn read_blob(
    &self,
    blob_id: super::BlobId,
    offset: super::BlobOffset,
    len: super::BlobLen,
  ) -> Result<AlignedBuf> {
    let blob = self
      .blob
      .get(blob_id)
      .unwrap_or_else(|| unreachable!("blob id {blob_id} must exists"));
    blob.read_at(offset, len)
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

  fn write_blob(&self, data: Vec<u8>) -> Result<super::BlobAppendGuard<'_>> {
    self.blob.append(data)
  }
}

pub fn initialize(
  block_cache: &BlockCache,
  tables: &TableMapper,
  recorder: &PageRecorder,
  version_visibility: &VersionVisibility,
  blob: &BlobStorage,
) -> Result {
  let policy = TableOpenPolicy {
    block_cache,
    version_visibility,
    recorder,
    blob,
  };
  BTreeIndex::new(policy).initialize(&tables.meta_table())?;
  Ok(())
}

pub fn open_tables(
  block_cache: &BlockCache,
  tables: &TableMapper,
  version_visibility: &VersionVisibility,
  blob: &BlobStorage,
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
    blob,
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

pub fn recovery(block_cache: Arc<BlockCache>, tables: &TableMapper) -> Result {
  let open_handles = Arc::new(SegQueue::new());
  tables
    .get_all()
    .into_iter()
    .for_each(|v| open_handles.push(v));

  let threads = (0..5)
    .map(|_| {
      let block_cache = block_cache.clone();
      let open_handles = open_handles.clone();
      once(move || {
        while let Some(table) = open_handles.pop() {
          debug!(
            "table {} start to collect orphaned blocks.",
            table.get_name(),
          );
          release_orphaned(&block_cache, &table)?;
        }
        Ok(())
      })
    })
    .collect::<Vec<_>>();

  threads.into_iter().try_for_each(|th| th.wait())?;
  info!("orphaned block has released successfully.");
  Ok(())
}

fn release_orphaned(block_cache: &BlockCache, table: &TableHandleRef) -> Result {
  let mut visited = HashSet::<Pointer>::from_iter([HEADER_POINTER]);
  let root = block_cache
    .read(HEADER_POINTER, table)?
    .for_read()
    .as_ref()
    .deserialize::<TreeHeader>()?
    .get_root();
  let mut node_stack = vec![root];
  let mut entry_stack = vec![];
  let mut used = HEADER_POINTER;

  while let Some(ptr) = node_stack.pop() {
    visited.insert(ptr);
    used = used.max(ptr);
    match block_cache
      .read(ptr, table)?
      .for_read()
      .as_ref()
      .view::<BTreeNodeView>()?
    {
      BTreeNodeView::Internal(node) => node_stack.extend(node.get_all_child()?),
      BTreeNodeView::Leaf(node) => {
        let mut iter = node.get_entries();
        while let Some((_, _, _, ptr)) = iter.try_next()? {
          entry_stack.push(ptr);
        }
      }
    };
  }

  while let Some(ptr) = entry_stack.pop() {
    visited.insert(ptr);
    used = used.max(ptr);
    if let Some(i) = block_cache
      .read(ptr, table)?
      .for_read()
      .as_ref()
      .view::<DataEntryView>()?
      .get_next()
    {
      entry_stack.push(i);
    };
  }

  (0..=used)
    .filter(|i| !visited.remove(i))
    .for_each(|i| table.free().dealloc(i));
  table.free().replay(used + 1);

  Ok(())
}
