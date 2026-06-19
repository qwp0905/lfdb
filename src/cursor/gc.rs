use std::{
  collections::{BTreeSet, LinkedList, VecDeque},
  sync::Arc,
  time::Duration,
};

use crossbeam::{epoch, queue::SegQueue};

use super::{
  BTreeNodeView, BlobId, BlobStorage, CompactionTriggered, DataEntry, DataEntryView,
  RecordData, RecordDataView, TreeHeader, VersionRecord, HEADER_POINTER,
};
use crate::{
  background::{
    BackgroundThread, EventBus, Oneshot, OwnedSubscription, SharedSubscription,
    WorkBuilder,
  },
  binding_events,
  cache::{BlockCache, RefedSlot},
  disk::Pointer,
  error,
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

pub struct GarbageCollector {
  release_queue: Arc<SegQueue<DropTableCommitted>>,
  main: Box<dyn BackgroundThread<(), Result>>,
  entry: Arc<dyn BackgroundThread<(TableHandleRef, Pointer), Result<EntryRelease>>>,
  table: Arc<dyn BackgroundThread<()>>,
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
    let entry = WorkBuilder::new()
      .name("gc found entry")
      .multi(config.thread_count)
      .shared(run_entry(
        block_cache.clone(),
        version_visibility.clone(),
        recorder.clone(),
      ))
      .to_arc();

    let table = WorkBuilder::new()
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

    let main = WorkBuilder::new()
      .name("gc main")
      .stack_size(2 << 20)
      .single()
      .interval(
        GC_RUN_INTERVAL,
        gc_main_loop(
          block_cache,
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

fn release_entry(
  block_cache: &BlockCache,
  version_visibility: &VersionVisibility,
  recorder: &PageRecorder,
  table: &TableHandleRef,
  pointer: Pointer,
) -> Result<EntryRelease> {
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
      let handle = table.clone();
      defer(move || handle.free().dealloc(ptr));
      continue;
    }

    {
      let mut found = false;
      let mut need_trim = false;
      let slot = block_cache.read(ptr, table)?.for_read();
      let entry = slot.as_ref().view::<DataEntryView>()?;
      next = entry.get_next();

      let prev_len = blob_refs.len();
      let mut iter = entry.get_versions();
      while let Some(record) = iter.try_next()? {
        if version_visibility.is_aborted(&record.owner) {
          need_trim = true;
          break;
        }
        if let RecordDataView::Blob(id, _, _) = record.data {
          blob_refs.push(id);
        }
        if record.version >= min_version {
          continue;
        }
        if !found {
          found = true;
          continue;
        }
        need_trim = true;
        break;
      }
      if !need_trim {
        continue;
      }
      blob_refs.drain(prev_len..);
    }

    block_cache.read(ptr, table)?.for_batch().mutate(|slot| {
      let mut entry: DataEntry = slot.as_ref().deserialize()?;

      let prev_len = entry.len();
      let mut expired_max: Option<VersionRecord> = None;
      let mut new_versions = VecDeque::new();

      for record in entry.take_versions() {
        if version_visibility.is_aborted(&record.owner) {
          continue;
        }
        if record.version >= min_version {
          new_versions.push_back(record);
          continue;
        }

        // Keep only the newest version at or below min_version. All active
        // transactions started after min_version, so older versions can never
        // be reached again.
        if expired_max
          .as_ref()
          .is_none_or(|max| max.version < record.version)
        {
          expired_max = Some(record);
        }
      }

      if let Some(record) = expired_max.take() {
        max_found = Some(record.version);
        new_versions.push_back(record);
      }

      for record in new_versions.iter() {
        let RecordData::Blob(id, _, _) = &record.data else {
          continue;
        };
        blob_refs.push(*id)
      }

      if new_versions.len() == prev_len {
        return Ok(());
      }

      if !new_versions.is_empty() {
        entry.set_versions(new_versions);
        serialize_and_log(slot, &entry)?;
        return Ok(());
      }

      let Some(next_ptr) = entry.get_next() else {
        return serialize_and_log(slot, &entry);
      };

      let next_entry = block_cache
        .read(next_ptr, table)?
        .for_read()
        .as_ref()
        .deserialize::<DataEntry>()?;
      serialize_and_log(slot, &next_entry)?;
      next = Some(ptr);

      let handle = table.clone();
      defer(move || handle.free().dealloc(next_ptr));
      Ok(())
    })?;
  }

  Ok(EntryRelease {
    min_version: max_found.unwrap_or(min_version),
    blob_refs,
  })
}

const fn run_entry(
  block_cache: Arc<BlockCache>,
  version_visibility: Arc<VersionVisibility>,
  recorder: Arc<PageRecorder>,
) -> impl Fn((TableHandleRef, Pointer)) -> Result<EntryRelease> {
  move |(table, pointer)| {
    release_entry(
      &block_cache,
      &version_visibility,
      &recorder,
      &table,
      pointer,
    )
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

fn defer<F, R>(f: F)
where
  F: FnOnce() -> R + Send + 'static,
{
  epoch::pin().defer(f)
}

struct GcCycle {
  tasks: ChunkQueue<GCTask>,
  min_version: TxId,
  blob_refs: BTreeSet<BlobId>,
}
impl GcCycle {
  fn new(min_version: TxId) -> Self {
    Self {
      tasks: ChunkQueue::new(),
      min_version,
      blob_refs: BTreeSet::new(),
    }
  }
}
struct GCTask {
  table: TableHandleRef,
  total: usize,
  dead: usize,
  leaf_ptr: Option<Pointer>,
}
impl GCTask {
  const fn new(table: TableHandleRef) -> Self {
    Self {
      table,
      total: 0,
      dead: 0,
      leaf_ptr: None,
    }
  }
}

fn gc_main_loop(
  block_cache: Arc<BlockCache>,
  tables: Arc<TableMapper>,
  version_visibility: Arc<VersionVisibility>,
  entry_worker: Arc<
    dyn BackgroundThread<(TableHandleRef, Pointer), Result<EntryRelease>>,
  >,
  event_bus: Arc<EventBus>,
  blob: Arc<BlobStorage>,
  key_count: usize,
  compaction_threshold: f64,
  compaction_min_size: Pointer,
) -> impl FnMut(Option<()>) -> Result {
  let mut cycle = None;
  let mut buffered = VecDeque::<Oneshot<Result<EntryRelease>>>::new();
  let flush =
    |buffered: &mut VecDeque<Oneshot<Result<EntryRelease>>>| -> Result<(TxId, Vec<BlobId>)> {
      let mut min = TxId::MAX;
      let mut refs = Vec::new();
      while let Some(handle) = buffered.pop_front() {
        let result = handle.wait().unwrap()?;
        min = min.min(result.min_version);
        refs.extend(result.blob_refs);
      }
      Ok((min, refs))
    };

  move |_| {
    let Some(current) = cycle.as_mut() else {
      let cycle = cycle.insert(GcCycle::new(version_visibility.min_version()));
      for task in tables.get_all().into_iter().map(GCTask::new) {
        cycle.tasks.push(task);
      }
      return Ok(());
    };

    for _ in 0..key_count {
      let Some(mut task) = current.tasks.pop() else {
        let (min, blob_refs) = flush(&mut buffered)?;
        for id in blob_refs {
          current.blob_refs.insert(id);
        }
        version_visibility.remove_aborted(&current.min_version.min(min));

        for handle in blob.readonly_handles() {
          if current.blob_refs.contains(&handle.get_id()) {
            continue;
          }
          blob.truncate(handle.get_id())?;
        }
        return Ok(cycle = None);
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

        loop {
          let slot = block_cache.read(ptr, table.handle())?.for_read();
          match slot.as_ref().view::<BTreeNodeView>()? {
            BTreeNodeView::Internal(node) => ptr = node.first_child()?,
            BTreeNodeView::Leaf(_) => break,
          }
        }

        task.leaf_ptr = Some(ptr);
        current.tasks.push(task);
        continue;
      };

      let slot = block_cache.read(ptr, table.handle())?.for_read();
      let node = slot.as_ref().view::<BTreeNodeView>()?.into_leaf()?;
      let mut iter = node.get_entries();
      while let Some((_, _, record, p)) = iter.try_next()? {
        current.min_version = current.min_version.min(record.version);
        buffered.push_back(entry_worker.execute((table.handle().clone(), p)));
        task.total += 1;

        if version_visibility.is_aborted(&record.owner) {
          task.dead += 1;
          continue;
        }
        match record.data {
          RecordDataView::Blob(id, _, _) => {
            current.blob_refs.insert(id);
          }
          RecordDataView::Tombstone => task.dead += 1,
          _ => {}
        }
      }

      if let Some(i) = node.get_next() {
        task.leaf_ptr = Some(i);
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

    let (min_version, blob_refs) = flush(&mut buffered)?;
    current.min_version = current.min_version.min(min_version);
    for id in blob_refs {
      current.blob_refs.insert(id);
    }
    Ok(())
  }
}
