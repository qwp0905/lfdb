use std::{
  collections::{HashSet, LinkedList, VecDeque},
  sync::Arc,
  time::Duration,
};

use crossbeam::{epoch::pin, queue::SegQueue};

use super::CompactionTriggered;
use crate::{
  background::{
    Close, EventBus, IntervalWorkThread, OwnedSubscription, SharedSubscription,
    ThreadBuilder,
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
  pub compact_threshold: f64,
  pub compact_min_size: Pointer,
}

const GC_RUN_INTERVAL: Duration = Duration::from_millis(500);

pub struct GarbageCollector {
  release_queue: Arc<SegQueue<DropTableCommitted>>,
  main: Box<IntervalWorkThread<()>>,
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

    let main = ThreadBuilder::new()
      .name("gc main")
      .stack_size(2 << 20)
      .single()
      .interval(
        GC_RUN_INTERVAL,
        gc_main_loop(worker, event_bus.clone(), release_queue.clone(), config),
      )
      .to_box();

    let this = Arc::new(Self {
      release_queue,
      main,
    });
    event_bus.register(&this);
    this
  }

  pub fn close(&self) {
    self.main.close();
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
  ) -> Result<EntryRelease> {
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

    Ok(EntryRelease {
      min_version: max_found.unwrap_or(min_version),
      blob_refs,
    })
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
    for task in self.mapper.get_all().into_iter().map(GcTask::uninit) {
      cycle.tasks.push(task);
    }
    cycle
  }
  fn finalize_cycle(&self, cycle: &mut GcCycle) -> Result {
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
    mut candidates: HashSet<StaticKey>,
    ptr: Pointer,
    task_queue: &mut ChunkQueue<GcTask>,
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
              let task = GcTask::new(TaskType::CheckEntry(ptr), table.clone());
              task_queue.push(task);
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
        let task = GcTask::new(TaskType::ReleaseEntry(ptr), table.clone());
        task_queue.push(task);
      }
    }

    Ok(())
  }

  fn run_tick(
    &self,
    cycle: &mut Option<GcCycle>,
    event_bus: &EventBus,
    config: GarbageCollectionConfig,
  ) -> Result {
    let Some(current) = cycle.as_mut() else {
      *cycle = Some(self.create_cycle());
      return Ok(());
    };

    for _ in 0..config.batch_size {
      let Some(mut task) = current.tasks.pop() else {
        self.finalize_cycle(current)?;
        *cycle = None;
        return Ok(());
      };

      let Some(table) = task.table.try_pin() else {
        continue;
      };

      let mut inner = match task.inner {
        TaskType::Uninit => {
          let ptr = self.get_predecessor(table.handle())?;
          task.inner = TaskType::CheckLeaf(CheckLeafTask::new(ptr));
          drop(table);
          current.tasks.push(task);
          continue;
        }
        TaskType::CheckEntry(ptr) => {
          let result = self.check_and_release_entry(ptr, table.handle())?;
          current.apply_release(result);
          continue;
        }
        TaskType::ReleaseEntry(ptr) => {
          self.release_entry(ptr, table.handle())?;
          continue;
        }
        TaskType::CheckLeaf(inner) => inner,
      };

      let mut release_candidates = HashSet::new();
      let has_next = {
        let min_version = self.version_visibility.min_version();
        let slot = self
          .block_cache
          .read(inner.pointer, table.handle())?
          .for_read();
        let node = slot.as_ref().view::<BTreeNodeView>()?.into_leaf()?;
        let mut iter = node.get_entries();
        while let Some(e) = iter.try_next()? {
          current.min_version = current.min_version.min(e.record.version);
          inner.total += 1;

          if self.version_visibility.is_aborted(&e.record.owner) {
            inner.dead += 1;
            if let Some(p) = e.next {
              let task = GcTask::new(TaskType::CheckEntry(p), table.handle().clone());
              current.tasks.push(task);
            }
            continue;
          }

          match e.record.data {
            RecordDataView::Blob(id, _, _) => {
              current.blob_refs.insert(id);
            }
            RecordDataView::Tombstone => inner.dead += 1,
            _ => {}
          }

          let Some(p) = e.next else {
            continue;
          };
          if e.record.version < min_version {
            release_candidates.insert(slot.as_ref().copy_range(e.range));
            continue;
          }

          let task = GcTask::new(TaskType::CheckEntry(p), table.handle().clone());
          current.tasks.push(task);
        }

        node.get_next()
      };

      self.release_leaf(
        table.handle(),
        release_candidates,
        inner.pointer,
        &mut current.tasks,
      )?;

      if let Some(p) = has_next {
        inner.pointer = p;
        task.inner = TaskType::CheckLeaf(inner);
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
      if inner.dead as f64 / inner.total as f64 <= config.compact_threshold {
        continue;
      }

      drop(table);
      event_bus.publish(CompactionTriggered::new(task.table));
    }

    Ok(())
  }

  fn release_tables(
    &self,
    release_queue: &SegQueue<DropTableCommitted>,
    steps: &mut TableSteps,
  ) -> Result {
    steps.ingest(release_queue);

    let min_version = self.version_visibility.min_version();
    steps.move_unreachable(min_version, |tx_id| {
      self.version_visibility.is_aborted(tx_id)
    });
    for table in steps.extract_unpinned() {
      table.truncate()?;
      self.mapper.remove(table.get_id());
    }
    Ok(())
  }
}

struct CheckLeafTask {
  pointer: Pointer,
  total: u64,
  dead: u64,
}
impl CheckLeafTask {
  const fn new(pointer: Pointer) -> Self {
    Self {
      pointer,
      total: 0,
      dead: 0,
    }
  }
}

enum TaskType {
  Uninit,
  CheckLeaf(CheckLeafTask),
  CheckEntry(Pointer),
  ReleaseEntry(Pointer),
}

struct GcTask {
  inner: TaskType,
  table: TableHandleRef,
}
impl GcTask {
  const fn uninit(table: TableHandleRef) -> Self {
    Self::new(TaskType::Uninit, table)
  }
  const fn new(inner: TaskType, table: TableHandleRef) -> Self {
    Self { inner, table }
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

  fn apply_release(&mut self, result: EntryRelease) {
    self.min_version = self.min_version.min(result.min_version);
    self.blob_refs.extend(result.blob_refs);
  }
}

struct TableSteps {
  incoming: LinkedList<(TableHandleRef, TxId, TxId)>,
  unreachable: LinkedList<TableHandleRef>,
}
impl TableSteps {
  const fn new() -> Self {
    Self {
      incoming: LinkedList::new(),
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
  fn extract_unpinned(&mut self) -> impl Iterator<Item = TableHandleRef> + '_ {
    self.unreachable.extract_if(|table| table.try_close())
  }
}

fn gc_main_loop(
  worker: Arc<GcWorker>,
  event_bus: Arc<EventBus>,
  release_queue: Arc<SegQueue<DropTableCommitted>>,
  config: GarbageCollectionConfig,
) -> impl FnMut(Option<()>) {
  let mut cycle = None;
  let mut steps = TableSteps::new();

  move |_| {
    worker
      .run_tick(&mut cycle, &event_bus, config.clone())
      .and_then(|_| worker.release_tables(&release_queue, &mut steps))
      .unwrap();
  }
}
