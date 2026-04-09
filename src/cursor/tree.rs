use std::{
  collections::HashSet,
  sync::{Arc, Mutex},
  time::Duration,
};

use super::{
  CursorNode, DataEntry, GarbageCollector, RecordData, TreeHeader, HEADER_POINTER,
};
use crate::{
  buffer_pool::BufferPool,
  disk::Pointer,
  table::{TableHandle, TableMapper, TableMetadata},
  thread::{once, BackgroundThread, WorkBuilder},
  transaction::{PageRecorder, VersionVisibility},
  utils::{LogFilter, ShortenedMutex, ToArc, ToBox},
  wal::RESERVED_TX,
  Result,
};

pub struct TreeManagerConfig {
  pub merge_interval: Duration,
}

/**
 * Compacts the B-tree by traversing from root to leaf nodes and reclaiming
 * empty entries and leaf pages.
 */
pub struct TreeManager {
  merge_leaf: Box<dyn BackgroundThread<(), Result>>,
}
impl TreeManager {
  fn new(
    buffer_pool: Arc<BufferPool>,
    tables: Arc<TableMapper>,
    recorder: Arc<PageRecorder>,
    gc: Arc<GarbageCollector>,
    logger: LogFilter,
    config: TreeManagerConfig,
  ) -> Self {
    let merge_leaf = WorkBuilder::new()
      .name("merge leaf nodes")
      .single()
      .interval(
        config.merge_interval,
        run_merge_leaf(
          buffer_pool.clone(),
          tables,
          recorder.clone(),
          gc.clone(),
          logger.clone(),
        ),
      )
      .to_box();

    Self { merge_leaf }
  }

  pub fn initialize(
    buffer_pool: Arc<BufferPool>,
    tables: Arc<TableMapper>,
    recorder: Arc<PageRecorder>,
    gc: Arc<GarbageCollector>,
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
        &CursorNode::initial_state(),
      )?;

      let mut header = buffer_pool.read(HEADER_POINTER, table.clone())?.for_write();
      recorder.serialize_and_log(
        RESERVED_TX,
        table_id,
        &mut header,
        &TreeHeader::new(root.get_pointer()),
      )?;
    }

    Ok(Self::new(buffer_pool, tables, recorder, gc, logger, config))
  }

  pub fn open_handles(
    buffer_pool: &BufferPool,
    version_visibility: &VersionVisibility,
    tables: &TableMapper,
  ) -> Result<Vec<(String, Arc<TableHandle>)>> {
    let mut handles = vec![];
    let current_version = version_visibility.current_version();
    let meta_table = tables.meta_table();

    let mut ptr = buffer_pool
      .read(HEADER_POINTER, meta_table.clone())?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    let leaf = loop {
      let node: CursorNode = buffer_pool
        .read(ptr, meta_table.clone())?
        .for_read()
        .as_ref()
        .deserialize()?;
      match node {
        CursorNode::Internal(internal) => ptr = internal.first_child(),
        CursorNode::Leaf(leaf) => break leaf,
      };
    };

    let mut next = Some(leaf);
    while let Some(leaf) = next.take() {
      if let Some(ptr) = leaf.get_next() {
        next = buffer_pool
          .read(ptr, meta_table.clone())?
          .for_read()
          .as_ref()
          .deserialize::<CursorNode>()?
          .as_leaf()
          .map(Some)?;
      }

      'outer: for (key, ptr) in leaf.get_entries() {
        let key = unsafe { String::from_utf8_unchecked(key.clone()) };
        let mut ptr = Some(*ptr);
        while let Some(p) = ptr.take() {
          let entry = buffer_pool
            .read(p, meta_table.clone())?
            .for_read()
            .as_ref()
            .deserialize::<DataEntry>()?;

          if let Some(record) =
            entry.find_record(current_version, |i| !version_visibility.is_aborted(i))
          {
            if let Some(bytes) = record.read_data(|i| {
              buffer_pool
                .read(i, meta_table.clone())?
                .for_read()
                .as_ref()
                .deserialize()
            })? {
              let metadata = TableMetadata::from_bytes(&bytes)?;
              let handle = tables.create_handle(metadata)?;
              handles.push((key, handle));
              continue 'outer;
            }
          };

          ptr = entry.get_next();
        }
      }
    }

    Ok(handles)
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
    logger: LogFilter,
    config: TreeManagerConfig,
  ) -> Result<Self> {
    let open_handles = Mutex::new(tables.get_all()).to_arc();

    let threads = (0..5)
      .map(|_| {
        let buffer_pool = buffer_pool.clone();
        let gc = gc.clone();
        let logger = logger.clone();
        let open_handles = open_handles.clone();
        once(move || {
          while let Some((name, table)) = open_handles.l().pop() {
            logger.debug(|| format!("{name} table start to collect orphand blocks."));
            release_orphand(&buffer_pool, &gc, &logger, table)?;
          }
          Ok(())
        })
      })
      .collect::<Vec<_>>();

    threads
      .into_iter()
      .map(|th| th.wait().flatten())
      .collect::<Result>()?;

    logger.info(|| "orphand block has released successfully.");
    Ok(Self::new(buffer_pool, tables, recorder, gc, logger, config))
  }

  pub fn close(&self) {
    self.merge_leaf.close();
  }
}

fn run_merge_leaf(
  buffer_pool: Arc<BufferPool>,
  tables: Arc<TableMapper>,
  recorder: Arc<PageRecorder>,
  gc: Arc<GarbageCollector>,
  logger: LogFilter,
) -> impl Fn(Option<()>) -> Result {
  move |_| {
    for (name, handle) in tables.get_all().into_iter() {
      logger.debug(|| format!("clean leaf {name} collect start."));
      let table_id = handle.metadata().get_id();

      let mut ptr = buffer_pool
        .peek(HEADER_POINTER, handle.clone())?
        .for_read()
        .as_ref()
        .deserialize::<TreeHeader>()?
        .get_root();

      while let CursorNode::Internal(node) = buffer_pool
        .peek(ptr, handle.clone())?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
      {
        ptr = node.first_child()
      }

      let mut index = Some(ptr);
      while let Some(i) = index.take() {
        {
          let leaf = buffer_pool
            .peek(i, handle.clone())?
            .for_read()
            .as_ref()
            .deserialize::<CursorNode>()?
            .as_leaf()?;
          if !gc
            .batch_check_empty(
              leaf
                .get_entry_pointers()
                .map(|p| (handle.clone(), p))
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

        let mut slot = buffer_pool.peek(i, handle.clone())?.for_write();
        let mut leaf = slot.as_ref().deserialize::<CursorNode>()?.as_leaf()?;
        index = leaf.get_next();

        let prev_len = leaf.len();
        let mut new_entries = vec![];
        let mut orphand = vec![];

        let found = leaf
          .drain()
          .map(|(key, ptr)| (key, ptr, gc.check_empty(handle.clone(), ptr)))
          .collect::<Vec<_>>();
        for (key, ptr, r) in found.into_iter() {
          match r.wait().flatten()? {
            true => orphand.push(ptr),
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

            orphand
              .into_iter()
              .for_each(|ptr| gc.lazy_release(handle.clone(), ptr));
            continue;
          }
        };

        // merge without propagating to internal nodes.
        logger.debug(|| {
          format!("trying to start merge {} with {}", slot.get_pointer(), next)
        });

        let mut next_slot = buffer_pool.peek(next, handle.clone())?.for_write();
        let next_leaf = next_slot.as_ref().deserialize::<CursorNode>()?;
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

        orphand
          .into_iter()
          .for_each(|ptr| gc.lazy_release(handle.clone(), ptr));
      }

      logger.debug(|| format!("clean leaf {name} collect end."));
    }
    Ok(())
  }
}

fn release_orphand(
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
      .deserialize::<CursorNode>()?
    {
      CursorNode::Internal(internal) => node_stack.extend(internal.get_all_child()),
      CursorNode::Leaf(leaf) => entry_stack.extend(leaf.get_entry_pointers()),
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
