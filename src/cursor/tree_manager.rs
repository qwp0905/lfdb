use std::{collections::HashSet, ops::Bound, sync::Arc, time::Duration};

use crossbeam::{epoch::pin, queue::SegQueue};

use super::{
  after_compaction, handle_compaction, wait_compaction, BTreeIndex, BTreeNode,
  BTreeNodeView, CompactTask, DataEntry, GarbageCollector, ReadonlyPolicy, RecordData,
  TreeHeader, COMPACTION_INTERVAL, HEADER_POINTER,
};
use crate::{
  cache::BlockCache,
  debug,
  disk::Pointer,
  info,
  table::{MutationHandle, TableHandle, TableMapper, TableMetadata, META_TABLE_ID},
  thread::{once, BackgroundThread, WorkBuilder},
  transaction::{PageRecorder, VersionVisibility},
  utils::{ToArc, ToBox},
  wal::{TxId, RESERVED_TX, WAL},
  Result,
};

pub struct TreeManagerConfig {
  pub merge_interval: Duration,
  pub compaction_threshold: f64,
  pub compaction_min_size: Pointer,
}

struct TableOpenPolicy<'a> {
  block_cache: &'a BlockCache,
  version_visibility: &'a VersionVisibility,
}
impl<'a> ReadonlyPolicy for TableOpenPolicy<'a> {
  fn is_visible(&self, owner: crate::wal::TxId, _: crate::wal::TxId) -> bool {
    !self.version_visibility.is_aborted(&owner)
  }

  fn fetch_slot(
    &self,
    pointer: Pointer,
    table: &Arc<TableHandle>,
  ) -> Result<crate::cache::CacheSlot<'_>> {
    self.block_cache.read(pointer, table.clone())
  }
}

/**
 * Compacts the B-tree by traversing from root to leaf nodes and reclaiming
 * empty entries and leaf pages.
 */
pub struct TreeManager {
  merge_leaf: Box<dyn BackgroundThread<(), Result>>,
  wait_compaction: Arc<dyn BackgroundThread<CompactTask, Result>>,
  do_compaction: Arc<dyn BackgroundThread<(MutationHandle, MutationHandle), Result>>,
  after_compaction:
    Arc<dyn BackgroundThread<(MutationHandle, MutationHandle, TxId, TxId)>>,
}
impl TreeManager {
  fn new(
    block_cache: Arc<BlockCache>,
    tables: Arc<TableMapper>,
    recorder: Arc<PageRecorder>,
    gc: Arc<GarbageCollector>,
    version_visibility: Arc<VersionVisibility>,
    wal: Arc<WAL>,
    config: TreeManagerConfig,
  ) -> Self {
    let after_compaction = WorkBuilder::new()
      .name("tree after compaction")
      .single()
      .interval(
        COMPACTION_INTERVAL,
        after_compaction(gc.clone(), version_visibility.clone()),
      )
      .to_arc();

    let do_compaction = WorkBuilder::new()
      .name("tree compaction")
      .multi(1)
      .shared(handle_compaction(
        tables.clone(),
        block_cache.clone(),
        version_visibility.clone(),
        wal.clone(),
        recorder.clone(),
        gc.clone(),
        after_compaction.clone(),
      ))
      .to_arc();

    let wait_compaction = WorkBuilder::new()
      .name("tree waiting compaction")
      .single()
      .interval(
        COMPACTION_INTERVAL,
        wait_compaction(
          tables.clone(),
          block_cache.clone(),
          version_visibility.clone(),
          wal.clone(),
          recorder.clone(),
          gc.clone(),
          do_compaction.clone(),
        ),
      )
      .to_arc();

    let merge_leaf = WorkBuilder::new()
      .name("merge leaf nodes")
      .single()
      .interval(
        config.merge_interval,
        run_merge_leaf(
          block_cache.clone(),
          tables.clone(),
          recorder.clone(),
          gc.clone(),
          wait_compaction.clone(),
          config.compaction_threshold,
          config.compaction_min_size,
        ),
      )
      .to_box();

    Self {
      merge_leaf,
      wait_compaction,
      do_compaction,
      after_compaction,
    }
  }

  pub fn initialize(
    block_cache: Arc<BlockCache>,
    tables: Arc<TableMapper>,
    recorder: Arc<PageRecorder>,
    gc: Arc<GarbageCollector>,
    wal: Arc<WAL>,
    version_visibility: Arc<VersionVisibility>,
    config: TreeManagerConfig,
  ) -> Result<Self> {
    let table = tables.meta_table();
    let table_id = table.metadata().get_id();
    {
      let root_ptr = table.free().alloc();
      let mut root = block_cache.read(root_ptr, table.clone())?.for_write();
      recorder.serialize_and_log(
        RESERVED_TX,
        table_id,
        &mut root,
        &BTreeNode::initial_state(),
      )?;

      let mut header = block_cache.read(HEADER_POINTER, table.clone())?.for_write();
      recorder.serialize_and_log(
        RESERVED_TX,
        table_id,
        &mut header,
        &TreeHeader::new(root.get_pointer()),
      )?;
    }

    Ok(Self::new(
      block_cache,
      tables,
      recorder,
      gc,
      version_visibility,
      wal,
      config,
    ))
  }

  pub fn open_handles(
    block_cache: &BlockCache,
    version_visibility: &VersionVisibility,
    tables: &TableMapper,
  ) -> Result<(Vec<Arc<TableHandle>>, Vec<(MutationHandle, MutationHandle)>)> {
    let mut handles = vec![];
    let mut compactions = vec![];
    let meta_table = tables.meta_table();

    let index = BTreeIndex::new(TableOpenPolicy {
      block_cache,
      version_visibility,
    });

    let mut iter = index.scan(&meta_table, &Bound::Unbounded, &Bound::Unbounded)?;

    while let Some((_, bytes)) = iter.next_kv_skip_tombstone()? {
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

  /**
   * Recovers orphaned pages on startup by traversing the entire B-tree and
   * collecting all reachable pages. Any page in [0, file_end) not reachable
   * is an orphan — typically a node allocated during a split, or a DataEntry
   * page allocated during an insert, where a crash occurred before the leaf
   * WAL record was written. Orphans are reclaimed via lazy_release.
   */
  pub fn clean_and_start(
    block_cache: Arc<BlockCache>,
    tables: Arc<TableMapper>,
    recorder: Arc<PageRecorder>,
    gc: Arc<GarbageCollector>,
    wal: Arc<WAL>,
    version_visibility: Arc<VersionVisibility>,
    config: TreeManagerConfig,
  ) -> Result<Self> {
    let open_handles = SegQueue::new().to_arc();
    tables
      .get_all()
      .into_iter()
      .for_each(|v| open_handles.push(v));

    let threads = (0..5)
      .map(|_| {
        let block_cache = block_cache.clone();
        let gc = gc.clone();
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
    Ok(Self::new(
      block_cache,
      tables,
      recorder,
      gc,
      version_visibility,
      wal,
      config,
    ))
  }

  pub fn close(&self) {
    self.merge_leaf.close();
    self.wait_compaction.close();
    self.do_compaction.close();
    self.after_compaction.close();
  }

  pub fn compact(&self, old: Arc<TableHandle>, new: MutationHandle, version: TxId) {
    self
      .wait_compaction
      .dispatch(CompactTask::Wait(old, new, version));
  }
  pub fn resume_compact(&self, old: MutationHandle, new: MutationHandle) {
    self.do_compaction.dispatch((old, new));
  }
}

fn run_merge_leaf(
  block_cache: Arc<BlockCache>,
  tables: Arc<TableMapper>,
  recorder: Arc<PageRecorder>,
  gc: Arc<GarbageCollector>,
  compaction: Arc<dyn BackgroundThread<CompactTask, Result>>,
  compaction_threshold: f64,
  compaction_min_size: Pointer,
) -> impl Fn(Option<()>) -> Result {
  move |_| {
    gc.run()?;

    for table in tables.get_all().into_iter() {
      let table = match table.try_pin() {
        Some(table) => table,
        None => continue,
      };

      let name = table.metadata().get_name();
      debug!("clean leaf table {name} collect start.",);
      let table_id = table.metadata().get_id();

      let mut ptr = block_cache
        .peek(HEADER_POINTER, table.handle())?
        .for_read(&pin())
        .as_ref()
        .deserialize::<TreeHeader>()?
        .get_root();

      while let BTreeNodeView::Internal(node) = block_cache
        .peek(ptr, table.handle())?
        .for_read(&pin())
        .as_ref()
        .view::<BTreeNodeView>()?
      {
        ptr = node.first_child()
      }

      let mut next_ptr = Some(ptr);
      while let Some(i) = next_ptr.take() {
        {
          let guard = pin();
          let slot = block_cache.peek(i, table.handle())?.for_read(&guard);
          let leaf = slot.as_ref().view::<BTreeNodeView>()?.as_leaf()?;
          if !gc
            .batch_check_empty(
              leaf
                .get_entry_pointers()
                .map(|p| (table.handle(), p))
                .collect(),
            )
            .wait()?
            .into_iter()
            .fold(Ok(false), |a, c| a.and_then(|a| c.map(|c| a || c)))?
          {
            next_ptr = leaf.get_next();
            continue;
          }
        }

        let mut slot = block_cache.peek(i, table.handle())?.for_write();
        let mut leaf = slot.as_ref().deserialize::<BTreeNode>()?.as_leaf()?;
        next_ptr = leaf.get_next();

        let prev_len = leaf.len();
        let mut new_entries = vec![];
        let mut orphaned = vec![];

        let found = leaf
          .drain()
          .map(|(key, ptr)| (key, ptr, gc.check_empty(table.handle(), ptr)))
          .collect::<Vec<_>>();
        for (key, ptr, r) in found.into_iter() {
          match r.wait().flatten()? {
            true => orphaned.push(ptr),
            false => new_entries.push((key, ptr)),
          }
        }

        let next = match (new_entries.len(), next_ptr) {
          (0, Some(next)) => next,
          (len, _) if len == prev_len => continue,
          _ => {
            leaf.set_entries(new_entries);
            recorder.serialize_and_log(
              RESERVED_TX,
              table_id,
              &mut slot,
              &leaf.to_node(),
            )?;
            drop(slot);

            orphaned
              .into_iter()
              .for_each(|ptr| gc.lazy_release(table.handle(), ptr));
            continue;
          }
        };

        // merge without propagating to internal nodes.
        debug!("trying to start merge {} with {}", slot.get_pointer(), next);

        let mut next_slot = block_cache.peek(next, table.handle())?.for_write();
        let next_leaf = next_slot.as_ref().deserialize::<BTreeNode>()?;
        leaf.set_next(slot.get_pointer());

        recorder.log_multi(
          RESERVED_TX,
          table_id,
          &mut slot,
          &next_leaf,
          &mut next_slot,
          &leaf.to_node(),
        )?;
        next_ptr = Some(slot.get_pointer());

        drop(slot);
        drop(next_slot);

        table.mark_redirection();
        orphaned
          .into_iter()
          .for_each(|ptr| gc.lazy_release(table.handle(), ptr));
      }

      if table.free().file_len() < compaction_min_size {
        continue;
      }

      let dead = table.dead_ratio();
      if dead > compaction_threshold && table_id != META_TABLE_ID {
        compaction.dispatch(CompactTask::New(table.handle()));
        continue;
      }

      debug!("clean leaf table {name} collect end. dead ratio {dead}");
    }
    Ok(())
  }
}

fn release_orphaned(
  block_cache: &BlockCache,
  gc: &GarbageCollector,
  table: Arc<TableHandle>,
) -> Result {
  let mut visited = HashSet::<Pointer>::from_iter([HEADER_POINTER]);
  let root = block_cache
    .read(HEADER_POINTER, table.clone())?
    .for_read(&pin())
    .as_ref()
    .deserialize::<TreeHeader>()?
    .get_root();
  let mut node_stack = vec![root];
  let mut entry_stack = vec![];

  while let Some(ptr) = node_stack.pop() {
    visited.insert(ptr);
    match block_cache
      .read(ptr, table.clone())?
      .for_read(&pin())
      .as_ref()
      .view::<BTreeNodeView>()?
    {
      BTreeNodeView::Internal(node) => node_stack.extend(node.get_all_child()),
      BTreeNodeView::Leaf(node) => {
        if node.len() == 0 {
          table.mark_redirection();
          continue;
        }
        entry_stack.extend(node.get_entry_pointers());
      }
    };
  }

  // push to queue for initial checkpoint
  entry_stack
    .iter()
    .for_each(|&ptr| gc.mark(table.clone(), ptr));
  debug!("{} entry queued for initial gc.", entry_stack.len());

  while let Some(ptr) = entry_stack.pop() {
    visited.insert(ptr);
    let entry: DataEntry = block_cache
      .read(ptr, table.clone())?
      .for_read(&pin())
      .as_ref()
      .deserialize()?;
    for record in entry.get_versions() {
      if let RecordData::Chunked(pointers) = &record.data {
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
    .for_each(|i| gc.lazy_release(table.clone(), i));

  Ok(())
}
