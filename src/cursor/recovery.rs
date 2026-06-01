use std::{collections::HashSet, ops::Bound, sync::Arc};

use crossbeam::queue::SegQueue;

use super::{
  BTreeIndex, BTreeNodeView, DataEntryView, GCMark, GCQueue, MergeSortable,
  ReadonlyPolicy, RecordDataView, TreeHeader, WritablePolicy, HEADER_POINTER,
};
use crate::{
  cache::BlockCache,
  debug,
  disk::Pointer,
  info,
  table::{MutationHandle, TableHandleRef, TableMapper, TableMetadata},
  thread::once,
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
    self.block_cache.read(pointer, table.clone())
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
      .serialize_and_log(RESERVED_TX, table.metadata().get_id(), slot, data)
  }

  fn alloc_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.block_cache.alloc(pointer, table.clone())
  }

  fn after_update_hook(&self, _pointer: Pointer, _table: &TableHandleRef) {}
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
) -> Result<(Vec<TableHandleRef>, Vec<(MutationHandle, MutationHandle)>)> {
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
        tables.create_handle(&metadata)?.try_mutation().unwrap(),
        tables.create_handle(&c_meta)?.try_mutation().unwrap(),
      )),
      None => handles.push(tables.create_handle(&metadata)?),
    }
  }

  Ok((handles, compactions))
}

pub fn recovery(
  block_cache: Arc<BlockCache>,
  gc_queue: Arc<GCQueue>,
  tables: &TableMapper,
) -> Result {
  let open_handles = Arc::new(SegQueue::new());
  tables
    .get_all()
    .into_iter()
    .for_each(|v| open_handles.push(v));

  let threads = (0..5)
    .map(|_| {
      let block_cache = block_cache.clone();
      let gc = gc_queue.clone();
      let open_handles = open_handles.clone();
      once(move || {
        while let Some(table) = open_handles.pop() {
          debug!(
            "table {} start to collect orphaned blocks.",
            table.metadata().get_name(),
          );
          release_orphaned(&block_cache, &gc, table)?;
        }
        Ok(())
      })
    })
    .collect::<Vec<_>>();

  threads
    .into_iter()
    .map(|th| th.wait().flatten())
    .collect::<Result>()?;

  info!("orphaned block has released successfully.");
  Ok(())
}

fn release_orphaned(
  block_cache: &BlockCache,
  gc: &GCQueue,
  table: TableHandleRef,
) -> Result {
  let mut visited = HashSet::<Pointer>::from_iter([HEADER_POINTER]);
  let root = block_cache
    .read(HEADER_POINTER, table.clone())?
    .for_read()
    .as_ref()
    .deserialize::<TreeHeader>()?
    .get_root();
  let mut node_stack = vec![root];
  let mut entry_stack = vec![];

  while let Some(ptr) = node_stack.pop() {
    visited.insert(ptr);
    match block_cache
      .read(ptr, table.clone())?
      .for_read()
      .as_ref()
      .view::<BTreeNodeView>()?
    {
      BTreeNodeView::Internal(node) => node_stack.extend(node.get_all_child()),
      BTreeNodeView::Leaf(node) => {
        for (_, _, record, ptr) in node.get_entries() {
          entry_stack.push(ptr);
          if let RecordDataView::Chunked(pointers) = &record.data {
            visited.extend(pointers);
          }
        }
      }
    };
  }

  for &ptr in entry_stack.iter() {
    gc.push(GCMark::new(ptr, table.clone(), RESERVED_TX));
  }

  while let Some(ptr) = entry_stack.pop() {
    visited.insert(ptr);
    let entry: DataEntryView = block_cache
      .read(ptr, table.clone())?
      .for_read()
      .as_ref()
      .deserialize()?;
    for record in entry.get_versions() {
      if let RecordDataView::Chunked(pointers) = &record.data {
        visited.extend(pointers);
      }
    }
    if let Some(i) = entry.get_next() {
      entry_stack.push(i)
    }
  }

  let file_end = table.disk().len()?;

  (0..file_end)
    .filter(|i| !visited.remove(i))
    .for_each(|i| table.free().dealloc(i));

  Ok(())
}
