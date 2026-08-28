use std::{
  collections::{HashSet, LinkedList, VecDeque},
  sync::Arc,
  time::Duration,
};

use crossbeam::{epoch::pin, queue::SegQueue};

use super::CompactionTriggered;
use crate::{
  background::{
    Close, EventBus, ForkStream, IntervalWorkThread, OwnedSubscription,
    SharedSubscription, StealingWorkThread, ThreadBuilder,
  },
  binding_events,
  blob::{BlobId, BlobStorage},
  cache::{BlockCache, RefedSlot},
  disk::Pointer,
  error,
  objects::{
    BTreeNode, BTreeNodeView, DataEntry, DataEntryView, RecordDataView, Serializable,
    StaticKey, TreeHeader, HEADER_POINTER,
  },
  table::{TableHandleRef, TableId, TableMapper},
  transaction::{PageRecorder, VersionVisibility},
  utils::{ChunkQueue, ToArc, ToBox},
  wal::{TxId, WALFailed, RESERVED_TX},
  Result,
};

#[derive(Clone)]
pub struct GarbageCollectionConfig {
  pub batch_size: usize,
  pub thread_count: usize,
  pub compact_threshold: f64,
  pub compact_min_size: Pointer,
}

const GC_RUN_INTERVAL: Duration = Duration::from_millis(500);

enum EntryWork {
  Check,
  Release,
}
type EntryWorkArg = (TableHandleRef, Pointer, EntryWork);
type EntryWorkResult = Result<Option<EntryRelease>>;
type EntryWorker = StealingWorkThread<EntryWorkArg, EntryWorkResult>;

pub struct GarbageCollector {
  release_queue: Arc<SegQueue<DropTableCommitted>>,
  main: Box<IntervalWorkThread<()>>,
  entry: Arc<EntryWorker>,
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
    let worker =
      GcWorker::new(block_cache, version_visibility, recorder, mapper, blob).to_arc();

    let entry = ThreadBuilder::new()
      .name("gc found entry")
      .multi(config.thread_count)
      .stealing(run_entry(worker.clone()))
      .to_arc();

    let main = ThreadBuilder::new()
      .name("gc main")
      .stack_size(2 << 20)
      .single()
      .interval(
        GC_RUN_INTERVAL,
        gc_main_loop(
          worker,
          entry.clone(),
          event_bus.clone(),
          release_queue.clone(),
          config,
        ),
      )
      .to_box();

    let this = Arc::new(Self {
      release_queue,
      main,
      entry,
    });
    event_bus.register(&this);
    this
  }

  pub fn close(&self) {
    self.main.close();
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

const fn run_entry(
  worker: Arc<GcWorker>,
) -> impl Fn((TableHandleRef, Pointer, EntryWork)) -> Result<Option<EntryRelease>> {
  move |(table, pointer, work)| match work {
    EntryWork::Check => worker.check_and_release_entry(pointer, &table),
    EntryWork::Release => worker.release_entry(pointer, &table).map(|_| None),
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

struct GcWorker {
  block_cache: Arc<BlockCache>,
  version_visibility: Arc<VersionVisibility>,
  recorder: Arc<PageRecorder>,
  mapper: Arc<TableMapper>,
  blob: Arc<BlobStorage>,
}
impl GcWorker {
  const fn new(
    block_cache: Arc<BlockCache>,
    version_visibility: Arc<VersionVisibility>,
    recorder: Arc<PageRecorder>,
    mapper: Arc<TableMapper>,
    blob: Arc<BlobStorage>,
  ) -> Self {
    Self {
      block_cache,
      version_visibility,
      recorder,
      mapper,
      blob,
    }
  }
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut RefedSlot,
    data: &T,
    table_id: TableId,
  ) -> Result {
    self
      .recorder
      .serialize_and_log(RESERVED_TX, table_id, RESERVED_TX, slot, data)
  }

  /**
   * First check whether this entry page has anything reclaimable. Most pages do
   * not need mutation, so the read-only pass avoids taking the batch/write path
   * unless trimming is actually required.
   */
  fn check_entry(
    &self,
    ptr: Pointer,
    table: &TableHandleRef,
    next: &mut Option<Pointer>,
    blob_refs: &mut Vec<BlobId>,
    min_version: TxId,
  ) -> Result<bool> {
    let mut found = false;
    let mut need_trim = false;
    let slot = self.block_cache.read(ptr, table)?.for_read();
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
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<Option<EntryRelease>> {
    let table_id = table.get_id();
    let mut next = Some(pointer);
    let mut max_found = None;
    let mut blob_refs = Vec::new();
    let min_version = self.version_visibility.min_version();

    while let Some(ptr) = next.take() {
      if max_found.is_some() {
        next = self
          .block_cache
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

      if !self.check_entry(ptr, table, &mut next, &mut blob_refs, min_version)? {
        continue;
      }

      self
        .block_cache
        .read(ptr, table)?
        .for_batch()
        .mutate(|slot| {
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
          self.serialize_and_log(slot, &entry, table_id)
        })?;
    }

    Ok(Some(EntryRelease {
      min_version: max_found.unwrap_or(min_version),
      blob_refs,
    }))
  }

  fn release_entry(&self, pointer: Pointer, table: &TableHandleRef) -> Result {
    let mut next = Some(pointer);
    while let Some(ptr) = next.take() {
      next = self
        .block_cache
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

  fn create_cycle(&self) -> GcCycle {
    let mut cycle = GcCycle::new(
      self.version_visibility.min_version(),
      self.blob.readonly_handle_ids(),
    );
    for task in self.mapper.get_all().into_iter().map(GcTask::new) {
      cycle.tasks.push(task);
    }
    cycle
  }
  fn finalize_cycle(
    &self,
    cycle: &mut GcCycle,
    stream: ForkStream<EntryWorkArg, EntryWorkResult>,
  ) -> Result {
    cycle.flush_stream(stream)?;
    self.version_visibility.remove_aborted(&cycle.min_version);

    for &id in cycle
      .exists_blobs
      .iter()
      .filter(|id| !cycle.blob_refs.contains(id))
    {
      self.blob.truncate(id)?;
    }
    Ok(())
  }

  fn get_predecessor(&self, table: &TableHandleRef) -> Result<Pointer> {
    let mut ptr = self
      .block_cache
      .read(HEADER_POINTER, table)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    while let BTreeNodeView::Internal(node) = self
      .block_cache
      .read(ptr, table)?
      .for_read()
      .as_ref()
      .view()?
    {
      ptr = node.first_child()?;
    }
    Ok(ptr)
  }

  fn release_leaf(
    &self,
    table: &TableHandleRef,
    stream: &mut ForkStream<EntryWorkArg, EntryWorkResult>,
    mut candidates: HashSet<StaticKey>,
    ptr: Pointer,
  ) -> Result {
    let count = candidates.len();
    if count == 0 {
      return Ok(());
    }

    let min_version = self.version_visibility.min_version();
    let mut next = Some(ptr);
    while let Some(ptr) = next.take() {
      let targets = self
        .block_cache
        .read(ptr, table)?
        .for_batch()
        .mutate(|slot| {
          let mut targets = Vec::new();
          let mut node = slot.as_ref().deserialize::<BTreeNode>()?;
          let leaf = node.as_leaf_mut()?;

          for entry in leaf.entries_mut().filter(|e| candidates.remove(&e.key)) {
            let Some(ptr) = entry.next else {
              continue;
            };

            if table.is_reserved(&entry.key)
              || entry.record.version >= min_version
              || self.version_visibility.is_aborted(&entry.record.owner)
            {
              stream.push((table.clone(), ptr, EntryWork::Check));
              continue;
            }

            targets.push(ptr);
            entry.next = None;
          }

          if !candidates.is_empty() {
            next = leaf.get_next();
          }

          if !targets.is_empty() {
            self.serialize_and_log(slot, &node, table.get_id())?;
          }
          Ok(targets)
        })?;
      for ptr in targets {
        stream.push((table.clone(), ptr, EntryWork::Release));
      }
    }

    Ok(())
  }

  fn run_tick(
    &self,
    cycle: &mut Option<GcCycle>,
    entry_worker: &EntryWorker,
    event_bus: &EventBus,
    config: GarbageCollectionConfig,
  ) -> Result {
    let Some(current) = cycle.as_mut() else {
      *cycle = Some(self.create_cycle());
      return Ok(());
    };

    let mut stream = entry_worker.stream();
    for _ in 0..config.batch_size {
      let Some(mut task) = current.tasks.pop() else {
        self.finalize_cycle(current, stream)?;
        *cycle = None;
        return Ok(());
      };

      let Some(table) = task.table.try_pin() else {
        continue;
      };

      let Some(ptr) = task.leaf_ptr.take() else {
        task.leaf_ptr = Some(self.get_predecessor(table.handle())?);
        drop(table);
        current.tasks.push(task);
        continue;
      };

      let mut release_candidates = HashSet::new();
      let has_next = {
        let min_version = self.version_visibility.min_version();
        let slot = self.block_cache.read(ptr, table.handle())?.for_read();
        let node = slot.as_ref().view::<BTreeNodeView>()?.into_leaf()?;
        let mut iter = node.get_entries();
        while let Some(e) = iter.try_next()? {
          current.min_version = current.min_version.min(e.record.version);
          task.total += 1;

          if self.version_visibility.is_aborted(&e.record.owner) {
            task.dead += 1;
            if let Some(p) = e.next {
              stream.push((table.handle().clone(), p, EntryWork::Check));
            }
            continue;
          }

          match e.record.data {
            RecordDataView::Blob(id, _, _) => {
              current.blob_refs.insert(id);
            }
            RecordDataView::Tombstone => task.dead += 1,
            _ => {}
          }

          let Some(p) = e.next else {
            continue;
          };
          if e.record.version < min_version {
            release_candidates.insert(slot.as_ref().copy_range(e.range));
            continue;
          }
          stream.push((table.handle().clone(), p, EntryWork::Check));
        }

        node.get_next()
      };

      self.release_leaf(table.handle(), &mut stream, release_candidates, ptr)?;

      if let Some(i) = has_next {
        task.leaf_ptr = Some(i);
        drop(table);
        current.tasks.push(task);
        continue;
      }

      if table.get_id() == self.mapper.meta_table_id() {
        continue;
      }
      if table.free().file_len() <= config.compact_min_size {
        continue;
      }
      if task.dead as f64 / task.total as f64 <= config.compact_threshold {
        continue;
      }

      event_bus.publish(CompactionTriggered::new(table.into_inner()));
    }

    current.flush_stream(stream)?;
    Ok(())
  }

  fn release_tables(
    &self,
    release_queue: &SegQueue<DropTableCommitted>,
    steps: &mut TableSteps,
  ) {
    steps.ingest(release_queue);

    let min_version = self.version_visibility.min_version();
    steps.move_unreachable(min_version, |tx_id| {
      self.version_visibility.is_aborted(tx_id)
    });
    steps.move_unpinned();

    for table in steps.extract_truncated() {
      self.mapper.remove(table.get_id());
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

  fn flush_stream(
    &mut self,
    stream: ForkStream<EntryWorkArg, EntryWorkResult>,
  ) -> Result {
    for result in stream.join() {
      let Some(result) = result? else {
        continue;
      };
      self.min_version = self.min_version.min(result.min_version);
      self.blob_refs.extend(result.blob_refs);
    }
    Ok(())
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

struct TableSteps {
  incoming: LinkedList<(TableHandleRef, TxId, TxId)>,
  unpinned: LinkedList<TableHandleRef>,
  unreachable: LinkedList<TableHandleRef>,
}
impl TableSteps {
  const fn new() -> Self {
    Self {
      incoming: LinkedList::new(),
      unpinned: LinkedList::new(),
      unreachable: LinkedList::new(),
    }
  }

  fn ingest(&mut self, queue: &SegQueue<DropTableCommitted>) {
    while let Some(committed) = queue.pop() {
      self.incoming.push_back((
        committed.handle,
        committed.owner,
        committed.commit_version,
      ));
    }
  }
  fn move_unreachable(&mut self, min_version: TxId, is_aborted: impl Fn(&TxId) -> bool) {
    for (table, _, _) in self
      .incoming
      .extract_if(|(_, tx_id, version)| is_aborted(tx_id) || min_version >= *version)
    {
      self.unreachable.push_back(table)
    }
  }
  fn move_unpinned(&mut self) {
    for table in self.unreachable.extract_if(|table| table.try_close()) {
      self.unpinned.push_back(table);
    }
  }
  fn extract_truncated(&mut self) -> impl Iterator<Item = TableHandleRef> + '_ {
    self.unpinned.extract_if(|table| table.truncate().is_ok())
  }
}

fn gc_main_loop(
  worker: Arc<GcWorker>,
  entry_worker: Arc<EntryWorker>,
  event_bus: Arc<EventBus>,
  release_queue: Arc<SegQueue<DropTableCommitted>>,
  config: GarbageCollectionConfig,
) -> impl FnMut(Option<()>) {
  let mut cycle = None;
  let mut steps = TableSteps::new();

  move |_| {
    worker
      .run_tick(&mut cycle, &entry_worker, &event_bus, config.clone())
      .unwrap();
    worker.release_tables(&release_queue, &mut steps);
  }
}
