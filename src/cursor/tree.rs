use std::{
  collections::{BTreeMap, HashSet},
  sync::{Arc, Mutex},
  time::Duration,
};

use super::{CursorNode, DataEntry, GarbageCollector, RecordData, TreeHeader};
use crate::{
  buffer_pool::BufferPool,
  disk::Pointer,
  serialize::Serializable,
  thread::{BackgroundThread, WorkBuilder},
  transaction::{FreeList, PageRecorder, TableMapper, VersionVisibility},
  utils::{LogFilter, ShortenedMutex, ToArc, ToBox},
  wal::RESERVED_TX,
  Error, Result,
};

pub const META_TABLE_HEADER: Pointer = 0;

pub struct TreeManagerConfig {
  pub merge_interval: Duration,
}

/**
 * Compacts the B-tree by traversing from root to leaf nodes and reclaiming
 * empty entries and leaf pages.
 */
pub struct TreeManager {
  release_tree: Box<dyn BackgroundThread<Pointer, Result>>,
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
      .stack_size(2 << 20)
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
    let release_tree = WorkBuilder::new()
      .name("release tree")
      .stack_size(2 << 20)
      .multi(1)
      .shared(run_release_tree(buffer_pool.clone(), gc.clone()))
      .to_box();
    Self {
      merge_leaf,
      release_tree,
    }
  }
  pub fn initial_state(
    free_list: &FreeList,
    buffer_pool: Arc<BufferPool>,
    tables: Arc<TableMapper>,
    recorder: Arc<PageRecorder>,
    gc: Arc<GarbageCollector>,
    logger: LogFilter,
    config: TreeManagerConfig,
  ) -> Result<Self> {
    {
      logger.info(|| "initialize metadata table.");

      let node_index = alloc_and_log(
        &free_list,
        &buffer_pool,
        &recorder,
        &CursorNode::initial_state(),
      )?;

      let root = TreeHeader::new(node_index);
      let mut root_slot = buffer_pool.read(META_TABLE_HEADER)?.for_write();
      recorder.serialize_and_log(RESERVED_TX, &mut root_slot, &root)?;
    }
    Ok(Self::new(buffer_pool, tables, recorder, gc, logger, config))
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
    version_visibility: &VersionVisibility,
    logger: LogFilter,
    config: TreeManagerConfig,
    file_end: Pointer,
  ) -> Result<Self> {
    let visited: Arc<Mutex<HashSet<Pointer>>> =
      Mutex::new(HashSet::from_iter(vec![META_TABLE_HEADER])).to_arc();

    let meta_root = buffer_pool
      .read(META_TABLE_HEADER)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();
    let mut node_stack = vec![meta_root];
    let mut key_values = vec![];

    while let Some(ptr) = node_stack.pop() {
      visited.l().insert(ptr);
      match buffer_pool
        .read(ptr)?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
      {
        CursorNode::Internal(internal) => node_stack.extend(internal.get_all_child()),
        CursorNode::Leaf(mut leaf) => key_values.extend(leaf.drain()),
      };
    }

    let tree_headers = Mutex::new(BTreeMap::new()).to_arc();
    let tx_id = version_visibility.current_version();

    while let Some((key, ptr)) = key_values.pop() {
      visited.l().insert(ptr);
      gc.mark(ptr);

      let entry: DataEntry = buffer_pool.read(ptr)?.for_read().as_ref().deserialize()?;
      for record in entry.get_versions() {
        if let RecordData::Chunked(pointers) = &record.data {
          visited.l().extend(pointers);
        }
      }
      if let Some(i) = entry.get_next() {
        key_values.push((key.clone(), i));
      }

      let mut tree_headers = tree_headers.l();
      if tree_headers.contains_key(&key) {
        continue;
      }

      let record = match entry.find_record(tx_id, |i| !version_visibility.is_aborted(&i))
      {
        Some(record) => record,
        None => continue,
      };

      let bytes = match record
        .read_data(|i| buffer_pool.read(i)?.for_read().as_ref().deserialize())?
      {
        Some(bytes) => bytes,
        None => continue,
      };
      let header = match bytes.try_into() {
        Ok(v) => Pointer::from_le_bytes(v),
        Err(_) => continue,
      };

      tree_headers.insert(key.clone(), header);
      tables.insert(unsafe { String::from_utf8_unchecked(key) }, header);
    }

    let threads = (0..5)
      .map(|_| {
        let buffer_pool = buffer_pool.clone();
        let gc = gc.clone();
        let logger = logger.clone();
        let visited = visited.clone();
        let tree_headers = tree_headers.clone();
        std::thread::spawn(move || {
          while let Some((_, header)) = tree_headers.l().pop_first() {
            tree_traversal(&buffer_pool, &gc, &logger, header, &visited)?;
          }
          Ok(()) as Result
        })
      })
      .collect::<Vec<_>>();

    threads
      .into_iter()
      .map(|th| th.join().map_err(Arc::from).map_err(Error::panic))
      .collect::<Result<Result>>()??;

    (0..file_end)
      .filter(|i| !visited.l().remove(i))
      .for_each(|i| gc.lazy_release(i));

    logger.info(|| "orphand block has released successfully.");
    Ok(Self::new(buffer_pool, tables, recorder, gc, logger, config))
  }

  pub fn release_tree(&self, header: Pointer) {
    self.release_tree.send(header);
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
    for (name, header) in tables
      .get_all()
      .into_iter()
      .chain([(format!("metadata"), META_TABLE_HEADER)])
    {
      logger.debug(|| format!("clean leaf {name} collect start."));

      let mut ptr = buffer_pool
        .peek(header)?
        .for_read()
        .as_ref()
        .deserialize::<TreeHeader>()?
        .get_root();

      while let CursorNode::Internal(node) = buffer_pool
        .peek(ptr)?
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
            .peek(i)?
            .for_read()
            .as_ref()
            .deserialize::<CursorNode>()?
            .as_leaf()?;
          if !gc
            .batch_check_empty(leaf.get_entry_pointers().collect())
            .wait()?
            .into_iter()
            .fold(Ok(false), |a, c| a.and_then(|a| c.map(|c| a || c)))?
          {
            index = leaf.get_next();
            continue;
          }
        }

        let mut slot = buffer_pool.peek(i)?.for_write();
        let mut leaf = slot.as_ref().deserialize::<CursorNode>()?.as_leaf()?;
        index = leaf.get_next();

        let prev_len = leaf.len();
        let mut new_entries = vec![];
        let mut orphand = vec![];

        let found = leaf
          .drain()
          .map(|(key, ptr)| (key, ptr, gc.check_empty(ptr)))
          .collect::<Vec<_>>();
        for (key, ptr, r) in found.into_iter() {
          match r.wait_flatten()? {
            true => orphand.push(ptr),
            false => new_entries.push((key, ptr)),
          }
        }

        let next = match (new_entries.len(), index) {
          (0, Some(next)) => next,
          (len, _) if len == prev_len => continue,
          _ => {
            leaf.set_entries(new_entries);
            recorder.serialize_and_log(RESERVED_TX, &mut slot, &leaf.to_node())?;
            drop(slot);

            orphand.into_iter().for_each(|ptr| gc.lazy_release(ptr));
            continue;
          }
        };

        // merge without propagating to internal nodes.
        logger.debug(|| {
          format!("trying to start merge {} with {}", slot.get_pointer(), next)
        });

        let mut next_slot = buffer_pool.peek(next)?.for_write();
        let next_leaf = next_slot.as_ref().deserialize::<CursorNode>()?;
        leaf.set_next(slot.get_pointer());

        recorder.log_multi(
          RESERVED_TX,
          &mut slot,
          &next_leaf,
          &mut next_slot,
          &leaf.to_node(),
        )?;
        index = Some(slot.get_pointer());

        drop(slot);
        drop(next_slot);

        orphand.into_iter().for_each(|ptr| gc.lazy_release(ptr));
      }

      logger.debug(|| format!("clean leaf {name} collect end."));
    }
    Ok(())
  }
}

fn alloc_and_log<T: Serializable>(
  free_list: &FreeList,
  buffer_pool: &BufferPool,
  recorder: &PageRecorder,
  data: &T,
) -> Result<Pointer> {
  let ptr = free_list.alloc();
  let mut slot = buffer_pool.peek(ptr)?.for_write();
  recorder.serialize_and_log(RESERVED_TX, &mut slot, data)?;
  Ok(ptr)
}

fn run_release_tree(
  buffer_pool: Arc<BufferPool>,
  gc: Arc<GarbageCollector>,
) -> impl Fn(Pointer) -> Result {
  move |header| {
    let root = buffer_pool
      .read(header)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    gc.lazy_release(header);
    let mut node_stack = vec![root];
    let mut entry_stack = vec![];

    while let Some(ptr) = node_stack.pop() {
      match buffer_pool
        .read(ptr)?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
      {
        CursorNode::Internal(internal) => node_stack.extend(internal.get_all_child()),
        CursorNode::Leaf(leaf) => entry_stack.extend(leaf.get_entry_pointers()),
      };
      gc.lazy_release(ptr);
    }

    while let Some(ptr) = entry_stack.pop() {
      let entry: DataEntry = buffer_pool.read(ptr)?.for_read().as_ref().deserialize()?;
      for record in entry.get_versions() {
        if let RecordData::Chunked(pointers) = &record.data {
          pointers.iter().for_each(|&p| gc.lazy_release(p));
        }
      }
      if let Some(i) = entry.get_next() {
        entry_stack.push(i)
      }
      gc.lazy_release(ptr);
    }

    Ok(())
  }
}

fn tree_traversal(
  buffer_pool: &BufferPool,
  gc: &GarbageCollector,
  logger: &LogFilter,
  header: Pointer,
  visited: &Mutex<HashSet<Pointer>>,
) -> Result {
  visited.l().insert(header);

  let root = buffer_pool
    .read(header)?
    .for_read()
    .as_ref()
    .deserialize::<TreeHeader>()?
    .get_root();
  let mut node_stack = vec![root];
  let mut entry_stack = vec![];

  while let Some(ptr) = node_stack.pop() {
    visited.l().insert(ptr);
    match buffer_pool
      .read(ptr)?
      .for_read()
      .as_ref()
      .deserialize::<CursorNode>()?
    {
      CursorNode::Internal(internal) => node_stack.extend(internal.get_all_child()),
      CursorNode::Leaf(leaf) => entry_stack.extend(leaf.get_entry_pointers()),
    };
  }

  // push to queue for initial checkpoint
  entry_stack.iter().for_each(|&ptr| gc.mark(ptr));
  logger.debug(|| format!("{} entry queued for initial gc.", entry_stack.len()));

  while let Some(ptr) = entry_stack.pop() {
    visited.l().insert(ptr);
    let entry: DataEntry = buffer_pool.read(ptr)?.for_read().as_ref().deserialize()?;
    for record in entry.get_versions() {
      if let RecordData::Chunked(pointers) = &record.data {
        visited.l().extend(pointers);
      }
    }
    if let Some(i) = entry.get_next() {
      entry_stack.push(i)
    }
  }

  Ok(())
}
