use std::{
  collections::{HashSet, LinkedList, VecDeque},
  sync::Arc,
  time::Duration,
};

use crossbeam::{epoch::pin, queue::SegQueue};

use super::CompactionTriggered;
use crate::{
  background::{
    BackgroundThread, EventBus, Oneshot, OwnedSubscription, SharedSubscription,
    ThreadBuilder,
  },
  binding_events,
  blob::{BlobId, BlobStorage},
  cache::{BlockCache, RefedSlot},
  disk::Pointer,
  error,
  objects::{
    BTreeNode, BTreeNodeView, DataEntry, DataEntryView, RecordDataView, StaticKey,
    TreeHeader, HEADER_POINTER,
  },
  table::{TableHandleRef, TableMapper, META_TABLE_ID},
  transaction::{PageRecorder, VersionVisibility},
  utils::{ChunkQueue, ToArc, ToBox},
  wal::{TxId, WALFailed, RESERVED_TX},
  Result,
};

pub struct GarbageCollectionConfig {
  pub batch_size: usize,
  pub thread_count: usize,
  pub compact_threshold: f64,
  pub compact_min_size: Pointer,
}

const RELEASE_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const GC_RUN_INTERVAL: Duration = Duration::from_millis(500);

enum EntryWork {
  Check,
  Release,
}
type EntryWorker =
  BackgroundThread<(TableHandleRef, Pointer, EntryWork), Result<Option<EntryRelease>>>;
pub struct GarbageCollector {
  release_queue: Arc<SegQueue<DropTableCommitted>>,
  main: Box<BackgroundThread<()>>,
  entry: Arc<EntryWorker>,
  table: Arc<BackgroundThread<()>>,
}
impl GarbageCollector {
  pub fn new(
    block_cache: Arc<BlockCache>,
    version_visibility: Arc<VersionVisibility>,
    recorder: Arc<PageRecorder>,
    mapper: Arc<TableMapper>,
    event_bus: Arc<EventBus>,
    blob: Arc<BlobStorage>,
    config: GarbageCollectionConfig,
  ) -> Arc<Self> {
    let release_queue = SegQueue::new().to_arc();
    let entry = ThreadBuilder::new()
      .name("gc found entry")
      .multi(config.thread_count)
      .shared(run_entry(
        block_cache.clone(),
        version_visibility.clone(),
        recorder.clone(),
      ))
      .to_arc();

    let table = ThreadBuilder::new()
      .name("gc release tables")
      .single()
      .interval(
        RELEASE_CHECK_INTERVAL,
        run_release_table(
          release_queue.clone(),
          mapper.clone(),
          version_visibility.clone(),
        ),
      )
      .to_arc();

    let main = ThreadBuilder::new()
      .name("gc main")
      .stack_size(2 << 20)
      .single()
      .interval(
        GC_RUN_INTERVAL,
        gc_main_loop(
          block_cache,
          recorder,
          mapper,
          version_visibility,
          entry.clone(),
          event_bus.clone(),
          blob,
          config.batch_size,
          config.compact_threshold,
          config.compact_min_size,
        ),
      )
      .to_box();

    let this = Arc::new(Self {
      release_queue,
      main,
      entry,
      table,
    });
    event_bus.register(&this);
    this
  }

  pub fn close(&self) {
    self.main.close();
    self.table.close();
    self.entry.close();
  }
}

impl OwnedSubscription<DropTableCommitted> for GarbageCollector {
  fn handle(&self, event: DropTableCommitted) {
    self.release_queue.push(event);
  }
}
impl SharedSubscription<WALFailed> for GarbageCollector {
  fn handle(&self, _: Arc<WALFailed>) {
    error!("garbage collector stopped since wal failure detected.");
    self.close();
  }
}
binding_events!(GarbageCollector {
  owned: [DropTableCommitted],
  shared: [WALFailed]
});

struct EntryRelease {
  min_version: TxId,
  blob_refs: Vec<BlobId>,
}

/**
 * First check whether this entry page has anything reclaimable. Most pages do
 * not need mutation, so the read-only pass avoids taking the batch/write path
 * unless trimming is actually required.
 */
fn check_entry(
  block_cache: &BlockCache,
  ptr: Pointer,
  table: &TableHandleRef,
  next: &mut Option<Pointer>,
  blob_refs: &mut Vec<BlobId>,
  min_version: TxId,
) -> Result<bool> {
  let mut found = false;
  let mut need_trim = false;
  let slot = block_cache.read(ptr, table)?.for_read();
  let entry = slot.as_ref().view::<DataEntryView>()?;
  *next = entry.get_next();

  let mut iter = entry.get_versions();
  while let Some(record) = iter.try_next()? {
    if found {
      need_trim = true;
      break;
    }
    if let RecordDataView::Blob(id, _, _) = record.data {
      blob_refs.push(id);
    }
    if record.version >= min_version {
      continue;
    }
    found = true;
  }

  Ok(need_trim || (found && entry.get_next().is_some()))
}

/**
 * Trim an entry chain while preserving the visibility boundary.
 *
 * `min_version` is the threshold visible to transactions that will start after
 * this point. GC may remove older history, but it must keep at least one record
 * below that threshold so future reads still have a stable version boundary.
 */
fn check_and_release_entry(
  block_cache: &BlockCache,
  version_visibility: &VersionVisibility,
  recorder: &PageRecorder,
  table: &TableHandleRef,
  pointer: Pointer,
) -> Result<Option<EntryRelease>> {
  let table_id = table.get_id();
  let mut next = Some(pointer);
  let mut max_found = None;
  let mut blob_refs = Vec::new();
  let min_version = version_visibility.min_version();

  let serialize_and_log = |slot: &mut RefedSlot, entry: &DataEntry| {
    recorder.serialize_and_log(RESERVED_TX, table_id, RESERVED_TX, slot, entry)
  };

  while let Some(ptr) = next.take() {
    if max_found.is_some() {
      next = block_cache
        .read(ptr, table)?
        .for_read()
        .as_ref()
        .view::<DataEntryView>()?
        .get_next();

      // lazily dealloc pointers since compaction can reachable.
      let guard = pin();
      let cloned = table.clone();
      guard.defer(move || cloned.free().dealloc(ptr));
      guard.flush();
      continue;
    }

    if !check_entry(
      block_cache,
      ptr,
      table,
      &mut next,
      &mut blob_refs,
      min_version,
    )? {
      continue;
    }

    block_cache.read(ptr, table)?.for_batch().mutate(|slot| {
      let mut entry: DataEntry = slot.as_ref().deserialize()?;
      let mut new_versions = VecDeque::new();

      for record in entry.take_versions() {
        let version = record.version;
        new_versions.push_back(record);
        if version >= min_version {
          continue;
        }
        max_found = Some(version);
        break;
      }

      if max_found.is_none() {
        return Ok(());
      }

      entry.set_versions(new_versions);
      entry.clear_next();
      serialize_and_log(slot, &entry)
    })?;
  }

  Ok(Some(EntryRelease {
    min_version: max_found.unwrap_or(min_version),
    blob_refs,
  }))
}

fn release_entry(
  block_cache: &BlockCache,
  table: &TableHandleRef,
  pointer: Pointer,
) -> Result {
  let mut next = Some(pointer);
  while let Some(ptr) = next.take() {
    next = block_cache
      .read(ptr, table)?
      .for_read()
      .as_ref()
      .view::<DataEntryView>()?
      .get_next();

    // lazily dealloc pointers since compaction can reachable.
    let guard = pin();
    let cloned = table.clone();
    guard.defer(move || cloned.free().dealloc(ptr));
    guard.flush();
  }
  Ok(())
}

const fn run_entry(
  block_cache: Arc<BlockCache>,
  version_visibility: Arc<VersionVisibility>,
  recorder: Arc<PageRecorder>,
) -> impl Fn((TableHandleRef, Pointer, EntryWork)) -> Result<Option<EntryRelease>> {
  move |(table, pointer, work)| match work {
    EntryWork::Check => check_and_release_entry(
      &block_cache,
      &version_visibility,
      &recorder,
      &table,
      pointer,
    ),
    EntryWork::Release => {
      release_entry(&block_cache, &table, pointer)?;
      Ok(None)
    }
  }
}

pub struct DropTableCommitted {
  handle: TableHandleRef,
  owner: TxId,
  commit_version: TxId,
}
impl DropTableCommitted {
  pub const fn new(handle: TableHandleRef, owner: TxId, commit_version: TxId) -> Self {
    Self {
      handle,
      owner,
      commit_version,
    }
  }
}

const fn run_release_table(
  queue: Arc<SegQueue<DropTableCommitted>>,
  mapper: Arc<TableMapper>,
  version_visibility: Arc<VersionVisibility>,
) -> impl FnMut(Option<()>) {
  let mut tables = LinkedList::new();
  let mut unpinned = LinkedList::new();
  let mut unreachable = LinkedList::new();
  move |_| {
    while let Some(committed) = queue.pop() {
      tables.push_back((committed.handle, committed.owner, committed.commit_version));
    }

    let min_version = version_visibility.min_version();
    for (table, _, _) in tables.extract_if(|(_, tx_id, version)| {
      version_visibility.is_aborted(tx_id) || min_version >= *version
    }) {
      unpinned.push_back(table)
    }

    for table in unpinned.extract_if(|table| table.try_close()) {
      unreachable.push_back(table);
    }

    for table in unreachable.extract_if(|table| table.truncate().is_ok()) {
      mapper.remove(table.get_id());
    }
  }
}

struct GcCycle {
  tasks: ChunkQueue<GcTask>,
  min_version: TxId,
  blob_refs: HashSet<BlobId>,
  exists_blobs: Vec<BlobId>,
}
impl GcCycle {
  fn new(min_version: TxId, exists_blobs: Vec<BlobId>) -> Self {
    Self {
      tasks: ChunkQueue::new(),
      min_version,
      blob_refs: HashSet::new(),
      exists_blobs,
    }
  }
}
struct GcTask {
  table: TableHandleRef,
  total: usize,
  dead: usize,
  leaf_ptr: Option<Pointer>,
}
impl GcTask {
  const fn new(table: TableHandleRef) -> Self {
    Self {
      table,
      total: 0,
      dead: 0,
      leaf_ptr: None,
    }
  }
}

fn flush_buffered(
  buffered: &mut VecDeque<Oneshot<Result<Option<EntryRelease>>>>,
  refs: &mut HashSet<BlobId>,
) -> Result<TxId> {
  let mut min = TxId::MAX;
  while let Some(handle) = buffered.pop_front() {
    let Some(result) = handle.wait().unwrap()? else {
      continue;
    };
    min = min.min(result.min_version);
    refs.extend(result.blob_refs);
  }
  Ok(min)
}
fn run_tick(
  cycle: &mut Option<GcCycle>,
  buffered: &mut VecDeque<Oneshot<Result<Option<EntryRelease>>>>,
  block_cache: &BlockCache,
  recorder: &PageRecorder,
  tables: &TableMapper,
  version_visibility: &VersionVisibility,
  entry_worker: &EntryWorker,
  event_bus: &EventBus,
  blob: &BlobStorage,
  key_count: usize,
  compaction_threshold: f64,
  compaction_min_size: Pointer,
) -> Result {
  let Some(current) = cycle.as_mut() else {
    let cycle = cycle.insert(GcCycle::new(
      version_visibility.min_version(),
      blob.readonly_handle_ids(),
    ));
    for task in tables.get_all().into_iter().map(GcTask::new) {
      cycle.tasks.push(task);
    }
    return Ok(());
  };

  for _ in 0..key_count {
    let Some(mut task) = current.tasks.pop() else {
      let min = flush_buffered(buffered, &mut current.blob_refs)?;
      version_visibility.remove_aborted(&current.min_version.min(min));

      for &id in current
        .exists_blobs
        .iter()
        .filter(|id| !current.blob_refs.contains(id))
      {
        blob.truncate(id)?;
      }
      *cycle = None;
      return Ok(());
    };

    let Some(table) = task.table.try_pin() else {
      continue;
    };

    let Some(ptr) = task.leaf_ptr.take() else {
      let mut ptr = block_cache
        .read(HEADER_POINTER, table.handle())?
        .for_read()
        .as_ref()
        .deserialize::<TreeHeader>()?
        .get_root();

      while let BTreeNodeView::Internal(node) = block_cache
        .read(ptr, table.handle())?
        .for_read()
        .as_ref()
        .view()?
      {
        ptr = node.first_child()?;
      }

      task.leaf_ptr = Some(ptr);
      drop(table);
      current.tasks.push(task);
      continue;
    };

    let mut release_candidates = HashSet::new();

    let has_next = {
      let min_version = version_visibility.min_version();
      let slot = block_cache.read(ptr, table.handle())?.for_read();
      let node = slot.as_ref().view::<BTreeNodeView>()?.into_leaf()?;
      let mut iter = node.get_entries();
      while let Some(e) = iter.try_next()? {
        current.min_version = current.min_version.min(e.record.version);
        task.total += 1;

        if version_visibility.is_aborted(&e.record.owner) {
          task.dead += 1;
          if let Some(p) = e.next {
            let handle =
              entry_worker.execute((table.handle().clone(), p, EntryWork::Check));
            buffered.push_back(handle);
          }
          continue;
        }

        if let Some(p) = e.next {
          if e.record.version < min_version {
            release_candidates.insert(slot.as_ref().copy_range(e.range));
          } else {
            let handle =
              entry_worker.execute((table.handle().clone(), p, EntryWork::Check));
            buffered.push_back(handle);
          }
        }

        match e.record.data {
          RecordDataView::Blob(id, _, _) => {
            current.blob_refs.insert(id);
          }
          RecordDataView::Tombstone => task.dead += 1,
          _ => {}
        }
      }

      node.get_next()
    };

    release_leaf(
      block_cache,
      recorder,
      version_visibility,
      table.handle(),
      entry_worker,
      buffered,
      release_candidates,
      ptr,
    )?;

    if let Some(i) = has_next {
      task.leaf_ptr = Some(i);
      drop(table);
      current.tasks.push(task);
      continue;
    }

    if table.get_id() == META_TABLE_ID {
      continue;
    }
    if table.free().file_len() <= compaction_min_size {
      continue;
    }
    if task.dead as f64 / task.total as f64 <= compaction_threshold {
      continue;
    }

    event_bus.publish(CompactionTriggered::new(table.into_inner()));
  }

  let min_version = flush_buffered(buffered, &mut current.blob_refs)?;
  current.min_version = current.min_version.min(min_version);
  Ok(())
}

fn release_leaf(
  block_cache: &BlockCache,
  recorder: &PageRecorder,
  version_visibility: &VersionVisibility,
  table: &TableHandleRef,
  entry_worker: &EntryWorker,
  buffered: &mut VecDeque<Oneshot<Result<Option<EntryRelease>>>>,
  mut candidates: HashSet<StaticKey>,
  ptr: Pointer,
) -> Result {
  let count = candidates.len();
  if count == 0 {
    return Ok(());
  }

  let min_version = version_visibility.min_version();
  let mut next = Some(ptr);
  while let Some(ptr) = next.take() {
    let targets = block_cache.read(ptr, table)?.for_batch().mutate(|slot| {
      let mut targets = Vec::new();
      let mut node = slot.as_ref().deserialize::<BTreeNode>()?;
      let leaf = node.as_leaf_mut()?;

      for entry in leaf.entries_mut().filter(|e| candidates.remove(&e.key)) {
        let Some(ptr) = entry.next else {
          continue;
        };

        if table.is_reserved(&entry.key)
          || entry.record.version >= min_version
          || version_visibility.is_aborted(&entry.record.owner)
        {
          let handle = entry_worker.execute((table.clone(), ptr, EntryWork::Check));
          buffered.push_back(handle);
          continue;
        }

        targets.push(ptr);
        entry.next = None;
      }

      if !candidates.is_empty() {
        next = leaf.get_next();
      }

      if !targets.is_empty() {
        recorder.serialize_and_log(
          RESERVED_TX,
          table.get_id(),
          RESERVED_TX,
          slot,
          &node,
        )?;
      }
      Ok(targets)
    })?;
    for ptr in targets {
      let handle = entry_worker.execute((table.clone(), ptr, EntryWork::Release));
      buffered.push_back(handle);
    }
  }

  Ok(())
}

fn gc_main_loop(
  block_cache: Arc<BlockCache>,
  recorder: Arc<PageRecorder>,
  tables: Arc<TableMapper>,
  version_visibility: Arc<VersionVisibility>,
  entry_worker: Arc<EntryWorker>,
  event_bus: Arc<EventBus>,
  blob: Arc<BlobStorage>,
  key_count: usize,
  compaction_threshold: f64,
  compaction_min_size: Pointer,
) -> impl FnMut(Option<()>) {
  let mut cycle = None;
  let mut buffered = VecDeque::new();

  move |_| {
    run_tick(
      &mut cycle,
      &mut buffered,
      &block_cache,
      &recorder,
      &tables,
      &version_visibility,
      &entry_worker,
      &event_bus,
      &blob,
      key_count,
      compaction_threshold,
      compaction_min_size,
    )
    .unwrap()
  }
}
