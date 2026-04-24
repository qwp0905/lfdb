use std::{collections::HashSet, ops::Bound, sync::Arc, time::Duration};

use crossbeam::queue::SegQueue;

use super::{
  handle_compaction, BTreeIndex, BTreeNode, BTreeNodeView, CompactTask, DataEntry,
  GarbageCollector, ReadonlyPolicy, RecordData, TreeHeader, HEADER_POINTER,
};
use crate::{
  buffer_pool::BufferPool,
  disk::Pointer,
  table::{MutationHandle, TableHandle, TableMapper, TableMetadata, META_TABLE_ID},
  thread::{once, BackgroundThread, WorkBuilder},
  transaction::{PageRecorder, VersionVisibility},
  utils::{LogFilter, ToArc, ToBox},
  wal::{RESERVED_TX, WAL},
  Result,
};

pub struct TreeManagerConfig {
  pub merge_interval: Duration,
  pub compaction_threshold: f64,
}

struct TableOpenPolicy<'a> {
  buffer_pool: &'a BufferPool,
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
  ) -> Result<crate::buffer_pool::Slot<'_>> {
    self.buffer_pool.read(pointer, table.clone())
  }
}

/**
 * Compacts the B-tree by traversing from root to leaf nodes and reclaiming
 * empty entries and leaf pages.
 */
pub struct TreeManager {
  merge_leaf: Box<dyn BackgroundThread<(), Result>>,
  compaction: Arc<dyn BackgroundThread<CompactTask, Result>>,
}
impl TreeManager {
  fn new(
    buffer_pool: Arc<BufferPool>,
    tables: Arc<TableMapper>,
    recorder: Arc<PageRecorder>,
    gc: Arc<GarbageCollector>,
    version_visibility: Arc<VersionVisibility>,
    wal: Arc<WAL>,
    logger: LogFilter,
    config: TreeManagerConfig,
  ) -> Self {
    let compaction = WorkBuilder::new()
      .name("tree compaction")
      .stack_size(2 << 20)
      .multi(1)
      .shared(handle_compaction(
        tables.clone(),
        buffer_pool.clone(),
        version_visibility.clone(),
        wal.clone(),
        recorder.clone(),
        gc.clone(),
        logger.clone(),
      ))
      .to_arc();

    let merge_leaf = WorkBuilder::new()
      .name("merge leaf nodes")
      .single()
      .interval(
        config.merge_interval,
        run_merge_leaf(
          buffer_pool.clone(),
          tables.clone(),
          recorder.clone(),
          gc.clone(),
          compaction.clone(),
          config.compaction_threshold,
          logger.clone(),
        ),
      )
      .to_box();

    Self {
      merge_leaf,
      compaction,
    }
  }

  pub fn initialize(
    buffer_pool: Arc<BufferPool>,
    tables: Arc<TableMapper>,
    recorder: Arc<PageRecorder>,
    gc: Arc<GarbageCollector>,
    wal: Arc<WAL>,
    version_visibility: Arc<VersionVisibility>,
    logger: LogFilter,
    config: TreeManagerConfig,
  ) -> Result<Self> {
    let table = tables.meta_table();
    let table_id = table.metadata().get_id();
    {
      let root_ptr = table.free().alloc();
      let mut root = buffer_pool.read(root_ptr, table.clone())?.for_write();
      recorder.serialize_and_log(
        RESERVED_TX,
        table_id,
        &mut root,
        &BTreeNode::initial_state(),
      )?;

      let mut header = buffer_pool.read(HEADER_POINTER, table.clone())?.for_write();
      recorder.serialize_and_log(
        RESERVED_TX,
        table_id,
        &mut header,
        &TreeHeader::new(root.get_pointer()),
      )?;
    }

    Ok(Self::new(
      buffer_pool,
      tables,
      recorder,
      gc,
      version_visibility,
      wal,
      logger,
      config,
    ))
  }

  pub fn open_handles(
    buffer_pool: &BufferPool,
    version_visibility: &VersionVisibility,
    tables: &TableMapper,
  ) -> Result<(
    Vec<Arc<TableHandle>>,
    Vec<(Arc<TableHandle>, MutationHandle)>,
  )> {
    let mut handles = vec![];
    let mut compactions = vec![];
    let meta_table = tables.meta_table();

    let index = BTreeIndex::new(TableOpenPolicy {
      buffer_pool,
      version_visibility,
    });

    let mut iter = index.scan(&meta_table, &Bound::Unbounded, &Bound::Unbounded)?;

    while let Some((_, bytes)) = iter.next_kv_skip_tombstone()? {
      let metadata = TableMetadata::from_bytes(&bytes)?;
      match metadata.get_compaction_metadata() {
        Some(c_meta) => compactions.push((
          tables.create_handle(&metadata)?,
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
    buffer_pool: Arc<BufferPool>,
    tables: Arc<TableMapper>,
    recorder: Arc<PageRecorder>,
    gc: Arc<GarbageCollector>,
    wal: Arc<WAL>,
    version_visibility: Arc<VersionVisibility>,
    logger: LogFilter,
    config: TreeManagerConfig,
  ) -> Result<Self> {
    let open_handles = SegQueue::new().to_arc();
    tables
      .get_all()
      .into_iter()
      .for_each(|v| open_handles.push(v));

    let threads = (0..5)
      .map(|_| {
        let buffer_pool = buffer_pool.clone();
        let gc = gc.clone();
        let logger = logger.clone();
        let open_handles = open_handles.clone();
        once(move || {
          while let Some(table) = open_handles.pop() {
            logger.debug(|| {
              format!(
                "table {} start to collect orphaned blocks.",
                table.metadata().get_name()
              )
            });
            release_orphaned(&buffer_pool, &gc, &logger, table)?;
          }
          Ok(())
        })
      })
      .collect::<Vec<_>>();

    threads
      .into_iter()
      .map(|th| th.wait().flatten())
      .collect::<Result>()?;

    logger.info(|| "orphaned block has released successfully.");
    Ok(Self::new(
      buffer_pool,
      tables,
      recorder,
      gc,
      version_visibility,
      wal,
      logger,
      config,
    ))
  }

  pub fn close(&self) {
    self.merge_leaf.close();
    self.compaction.close();
  }

  pub fn compact(&self, old: Arc<TableHandle>, new: MutationHandle) {
    self.compaction.dispatch(CompactTask::Resume(old, new));
  }
}

fn run_merge_leaf(
  buffer_pool: Arc<BufferPool>,
  tables: Arc<TableMapper>,
  recorder: Arc<PageRecorder>,
  gc: Arc<GarbageCollector>,
  compaction: Arc<dyn BackgroundThread<CompactTask, Result>>,
  compaction_threshold: f64,
  logger: LogFilter,
) -> impl Fn(Option<()>) -> Result {
  move |_| {
    for table in tables.get_all().into_iter() {
      let table = match table.try_pin() {
        Some(table) => table,
        None => continue,
      };

      logger.debug(|| {
        format!(
          "clean leaf table {} collect start.",
          table.metadata().get_name()
        )
      });
      let table_id = table.metadata().get_id();

      let mut ptr = buffer_pool
        .peek(HEADER_POINTER, table.handle())?
        .for_read()
        .as_ref()
        .deserialize::<TreeHeader>()?
        .get_root();

      while let BTreeNodeView::Internal(node) = buffer_pool
        .peek(ptr, table.handle())?
        .for_read()
        .as_ref()
        .view::<BTreeNodeView>()?
      {
        ptr = node.first_child()
      }

      let mut index = Some(ptr);
      while let Some(i) = index.take() {
        {
          let slot = buffer_pool.peek(i, table.handle())?.for_read();
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
            index = leaf.get_next();
            continue;
          }
        }

        let mut slot = buffer_pool.peek(i, table.handle())?.for_write();
        let mut leaf = slot.as_ref().deserialize::<BTreeNode>()?.as_leaf()?;
        index = leaf.get_next();

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

        let next = match (new_entries.len(), index) {
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
        logger.debug(|| {
          format!("trying to start merge {} with {}", slot.get_pointer(), next)
        });

        let mut next_slot = buffer_pool.peek(next, table.handle())?.for_write();
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
        index = Some(slot.get_pointer());

        drop(slot);
        drop(next_slot);

        table.mark_redirection();
        orphaned
          .into_iter()
          .for_each(|ptr| gc.lazy_release(table.handle(), ptr));
      }

      let dead = table.dead_ratio();
      let name = table.metadata().get_name();
      if dead > compaction_threshold && table_id != META_TABLE_ID {
        logger.debug(|| format!("table {name} dead ratio exceeded as {dead}.",));
        compaction.dispatch(CompactTask::New(table.handle()));
        continue;
      }

      logger.debug(|| format!("clean leaf table {name} collect end. dead ratio {dead}",));
    }
    Ok(())
  }
}

fn release_orphaned(
  buffer_pool: &BufferPool,
  gc: &GarbageCollector,
  logger: &LogFilter,
  table: Arc<TableHandle>,
) -> Result {
  let mut visited = HashSet::<Pointer>::from_iter([HEADER_POINTER]);
  let root = buffer_pool
    .read(HEADER_POINTER, table.clone())?
    .for_read()
    .as_ref()
    .deserialize::<TreeHeader>()?
    .get_root();
  let mut node_stack = vec![root];
  let mut entry_stack = vec![];

  while let Some(ptr) = node_stack.pop() {
    visited.insert(ptr);
    match buffer_pool
      .read(ptr, table.clone())?
      .for_read()
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
  logger.debug(|| format!("{} entry queued for initial gc.", entry_stack.len()));

  while let Some(ptr) = entry_stack.pop() {
    visited.insert(ptr);
    let entry: DataEntry = buffer_pool
      .read(ptr, table.clone())?
      .for_read()
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
