use std::{
  collections::{HashSet, LinkedList, VecDeque},
  sync::Arc,
  time::Duration,
};

use super::CompactionTriggered;
use crate::{
  background::{
    binding_events, EventBus, IntervalWorkThread, OwnedSubscription, SharedSubscription,
    ThreadBuilder,
  },
  blob::{BlobId, BlobStorage},
  cache::{BlockCache, PreparedSlot},
  disk::Pointer,
  mvcc::VersionController,
  objects::{
    BTreeNode, BTreeNodeView, DataEntry, DataEntryView, RecordDataView, Serializable,
    StaticKey, TreeHeader, HEADER_POINTER,
  },
  page::PAGE_SIZE,
  table::{TableHandleRef, TableId, TableMapper},
  transaction::PageRecorder,
  utils::{error, ChunkQueue, MpscQueue, ToArc, ToBox},
  wal::{TxId, WALFailed, RESERVED_TX},
  Error, Result,
};

pub struct GarbageCollectionConfig {
  pub batch_size: usize,
  pub compact_threshold: f64,
  pub compact_min_size: usize,
}

const GC_RUN_INTERVAL: Duration = Duration::from_millis(500);

pub struct GarbageCollector {
  release_queue: Arc<MpscQueue<DropTableCommitted>>,
  main: Box<IntervalWorkThread>,
}
impl GarbageCollector {
  pub fn new(
    block_cache: Arc<BlockCache>,
    version_controller: Arc<VersionController>,
    recorder: Arc<PageRecorder>,
    mapper: Arc<TableMapper>,
    event_bus: Arc<EventBus>,
    blob: Arc<BlobStorage>,
    config: GarbageCollectionConfig,
  ) -> Arc<Self> {
    let release_queue = MpscQueue::new().to_arc();
    let worker =
      GcWorker::new(block_cache, version_controller, recorder, mapper, blob).to_arc();

    let main = ThreadBuilder::new()
      .name("gc main")
      .stack_size(2 << 20)
      .single()
      .interval(
        GC_RUN_INTERVAL,
        gc_main_loop(
          worker,
          event_bus.clone(),
          release_queue.clone(),
          config.batch_size,
          config.compact_threshold,
          (config.compact_min_size / PAGE_SIZE) as Pointer,
        ),
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

struct TrimEntry {
  min_version: TxId,
  blob_refs: Vec<BlobId>,
  tail: Option<Pointer>,
  found: bool,
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
  version_controller: Arc<VersionController>,
  recorder: Arc<PageRecorder>,
  mapper: Arc<TableMapper>,
  blob: Arc<BlobStorage>,
}
impl GcWorker {
  const fn new(
    block_cache: Arc<BlockCache>,
    version_controller: Arc<VersionController>,
    recorder: Arc<PageRecorder>,
    mapper: Arc<TableMapper>,
    blob: Arc<BlobStorage>,
  ) -> Self {
    Self {
      block_cache,
      version_controller,
      recorder,
      mapper,
      blob,
    }
  }
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut PreparedSlot,
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
    blob_refs: &mut Vec<BlobId>,
    min_version: TxId,
  ) -> Result<(bool, Option<Pointer>)> {
    let mut found = false;
    let mut need_trim = false;
    let slot = self.block_cache.read(ptr, table)?.for_read();
    let entry = slot.as_ref().view::<DataEntryView>()?;

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

    let next = entry.get_next();
    Ok((need_trim || (found && next.is_some()), next))
  }

  /**
   * Trim an entry chain while preserving the visibility boundary.
   *
   * `min_version` is the threshold visible to transactions that will start after
   * this point. GC may remove older history, but it must keep at least one record
   * below that threshold so future reads still have a stable version boundary.
   */
  fn trim_entry(&self, pointer: Pointer, table: &TableHandleRef) -> Result<TrimEntry> {
    let table_id = table.get_id();
    let mut blob_refs = Vec::new();
    let min_version = self.version_controller.min_version();

    let (need_trim, tail) =
      self.check_entry(pointer, table, &mut blob_refs, min_version)?;
    if !need_trim {
      return Ok(TrimEntry {
        min_version,
        blob_refs,
        tail,
        found: false,
      });
    };

    let mut max_found = None;
    let mut slot = self.block_cache.read(pointer, table)?.for_write();
    let mut entry: DataEntry = slot.as_ref().deserialize()?;
    let mut new_versions = VecDeque::new();
    let tail = entry.get_next();

    for record in entry.take_versions() {
      let version = record.version;
      new_versions.push_back(record);
      if version >= min_version {
        continue;
      }

      max_found = Some(version);
      break;
    }

    let Some(max_found) = max_found else {
      return Ok(TrimEntry {
        min_version,
        blob_refs,
        tail,
        found: false,
      });
    };

    entry.set_versions(new_versions);
    entry.clear_next();
    self.serialize_and_log(slot.as_prepared(), &entry, table_id)?;
    Ok(TrimEntry {
      min_version: max_found,
      blob_refs,
      tail,
      found: true,
    })
  }

  fn release_entry(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<Option<Pointer>> {
    let next = self
      .block_cache
      .read(pointer, table)?
      .for_read()
      .as_ref()
      .view::<DataEntryView>()?
      .get_next();
    table.free().dealloc(pointer);
    Ok(next)
  }

  fn create_cycle(&self) -> GcCycle {
    let mut cycle = GcCycle::new(
      self.version_controller.min_version(),
      self.blob.readonly_handle_ids(),
    );
    for task in self.mapper.get_all().into_iter().map(GcTask::uninit) {
      cycle.tasks.push(task);
    }
    cycle
  }
  fn finalize_cycle(&self, cycle: &mut GcCycle) -> Result {
    self.version_controller.remove_aborted(cycle.min_version);
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
  ) -> Result<Vec<GcTask>> {
    let mut buffered_task = Vec::new();
    let count = candidates.len();
    if count == 0 {
      return Ok(buffered_task);
    }

    let min_version = self.version_controller.min_version();
    let mut next = Some(ptr);
    while let Some(ptr) = next.take() {
      let mut targets = Vec::new();
      {
        let mut slot = self.block_cache.read(ptr, table)?.for_write();

        let mut node = slot.as_ref().deserialize::<BTreeNode>()?;
        let leaf = node.as_leaf_mut()?;

        for entry in leaf.entries_mut().filter(|e| candidates.remove(&e.key)) {
          let Some(ptr) = entry.next else {
            continue;
          };

          if table.is_reserved(&entry.key)
            || entry.record.version >= min_version
            || self.version_controller.is_aborted(entry.record.owner)
          {
            let task = GcTask::trim_entry(ptr, table.clone());
            buffered_task.push(task);
            continue;
          }

          targets.push(ptr);
          entry.next = None;
        }

        if !candidates.is_empty() {
          next = leaf.get_next();
        }

        if !targets.is_empty() {
          self.serialize_and_log(slot.as_prepared(), &node, table.get_id())?;
        }
      };
      for ptr in targets {
        let task = GcTask::release_entry(ptr, table.clone());
        task_queue.push(task);
      }
    }

    Ok(buffered_task)
  }

  fn handle_task(
    &self,
    current: &mut GcCycle,
    task: &GcTask,
    event_bus: &EventBus,
    compact_threshold: f64,
    compact_min_size: Pointer,
  ) -> Result {
    let Some(_token) = task.table.try_pin() else {
      return Ok(());
    };

    let inner = match &task.inner {
      TaskType::Uninit => {
        let ptr = self.get_predecessor(&task.table)?;
        let task = GcTask::check_leaf_init(ptr, task.table.clone());
        current.tasks.push(task);
        return Ok(());
      }
      TaskType::TrimEntry(ptr) => {
        let result = self.trim_entry(*ptr, &task.table)?;
        current.apply_release(result, &task.table);
        return Ok(());
      }
      TaskType::ReleaseEntry(ptr) => {
        if let Some(ptr) = self.release_entry(*ptr, &task.table)? {
          let task = GcTask::release_entry(ptr, task.table.clone());
          current.tasks.push(task);
        };
        return Ok(());
      }
      TaskType::CheckLeaf(inner) => inner,
    };

    let mut total = inner.total;
    let mut dead = inner.dead;
    let mut tasks = Vec::new();

    let mut release_candidates = HashSet::new();
    let has_next = {
      let min_version = self.version_controller.min_version();
      let slot = self
        .block_cache
        .read(inner.pointer, &task.table)?
        .for_read();
      let node = slot.as_ref().view::<BTreeNodeView>()?.into_leaf()?;
      let mut iter = node.get_entries();
      while let Some(e) = iter.try_next()? {
        current.min_version = current.min_version.min(e.record.version);
        total += 1;

        if self.version_controller.is_aborted(e.record.owner) {
          dead += 1;
          if let Some(p) = e.next {
            let task = GcTask::trim_entry(p, task.table.clone());
            tasks.push(task);
          }
          continue;
        }

        match e.record.data {
          RecordDataView::Blob(id, _, _) => {
            current.blob_refs.insert(id);
          }
          RecordDataView::Tombstone => dead += 1,
          _ => {}
        }

        let Some(p) = e.next else {
          continue;
        };
        if e.record.version < min_version {
          release_candidates.insert(slot.as_ref().copy_range(e.range));
          continue;
        }

        let task = GcTask::trim_entry(p, task.table.clone());
        tasks.push(task);
      }

      node.get_next()
    };

    let buffered = self.release_leaf(
      &task.table,
      release_candidates,
      inner.pointer,
      &mut current.tasks,
    )?;
    for task in tasks.into_iter().chain(buffered) {
      current.tasks.push(task);
    }

    if let Some(p) = has_next {
      let task = GcTask::check_leaf_resume(p, task.table.clone(), total, dead);
      current.tasks.push(task);
      return Ok(());
    }

    if task.table.get_id() == self.mapper.meta_table_id() {
      return Ok(());
    }
    if task.table.free().file_len() <= compact_min_size {
      return Ok(());
    }
    if total != 0 && (dead as f64 / total as f64 <= compact_threshold) {
      return Ok(());
    }
    event_bus.publish(CompactionTriggered::new(task.table.clone()));
    Ok(())
  }

  fn run_tick(
    &self,
    cycle: &mut Option<GcCycle>,
    event_bus: &EventBus,
    batch_size: usize,
    compact_threshold: f64,
    compact_min_size: Pointer,
  ) -> Result {
    let Some(current) = cycle.as_mut() else {
      *cycle = Some(self.create_cycle());
      return Ok(());
    };

    for _ in 0..batch_size {
      let Some(task) = current.tasks.pop() else {
        self.finalize_cycle(current)?;
        *cycle = None;
        return Ok(());
      };

      if let Err(err) = self.handle_task(
        current,
        &task,
        event_bus,
        compact_threshold,
        compact_min_size,
      ) {
        current.tasks.push(task);
        return Err(err);
      };
    }

    Ok(())
  }

  fn release_tables(
    &self,
    release_queue: &MpscQueue<DropTableCommitted>,
    steps: &mut TableSteps,
  ) {
    steps.ingest(release_queue);

    let min_version = self.version_controller.min_version();
    steps.move_unreachable(min_version, |&tx_id| {
      self.version_controller.is_aborted(tx_id)
    });

    let mut failed = Vec::new();
    for table in steps.extract_unpinned() {
      if let Err(err) = table.truncate() {
        error!("gc release table failed: {err}");
        failed.push(table);
        continue;
      };

      self.mapper.remove(table.get_id());
    }
    for table in failed {
      steps.abort_unpinned(table);
    }
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

  const fn resume(pointer: Pointer, total: u64, dead: u64) -> Self {
    Self {
      pointer,
      total,
      dead,
    }
  }
}

enum TaskType {
  Uninit,
  CheckLeaf(CheckLeafTask),
  TrimEntry(Pointer),
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
  const fn check_leaf_init(ptr: Pointer, table: TableHandleRef) -> Self {
    Self::new(TaskType::CheckLeaf(CheckLeafTask::new(ptr)), table)
  }
  const fn check_leaf_resume(
    ptr: Pointer,
    table: TableHandleRef,
    total: u64,
    dead: u64,
  ) -> Self {
    Self::new(
      TaskType::CheckLeaf(CheckLeafTask::resume(ptr, total, dead)),
      table,
    )
  }
  const fn release_entry(ptr: Pointer, table: TableHandleRef) -> Self {
    Self::new(TaskType::ReleaseEntry(ptr), table)
  }
  const fn trim_entry(ptr: Pointer, table: TableHandleRef) -> Self {
    Self::new(TaskType::TrimEntry(ptr), table)
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

  fn apply_release(&mut self, result: TrimEntry, table: &TableHandleRef) {
    self.min_version = self.min_version.min(result.min_version);
    self.blob_refs.extend(result.blob_refs);
    let Some(ptr) = result.tail else {
      return;
    };
    let task = if result.found {
      GcTask::release_entry(ptr, table.clone())
    } else {
      GcTask::trim_entry(ptr, table.clone())
    };
    self.tasks.push(task);
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

  fn ingest(&mut self, queue: &MpscQueue<DropTableCommitted>) {
    while let Some(committed) = unsafe { queue.pop() } {
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
  fn abort_unpinned(&mut self, table: TableHandleRef) {
    self.unreachable.push_back(table);
  }
}

const fn gc_main_loop(
  worker: Arc<GcWorker>,
  event_bus: Arc<EventBus>,
  release_queue: Arc<MpscQueue<DropTableCommitted>>,
  batch_size: usize,
  compact_threshold: f64,
  compact_min_size: Pointer,
) -> impl FnMut() {
  let mut cycle = None;
  let mut steps = TableSteps::new();

  move || {
    let Err(err) = worker.run_tick(
      &mut cycle,
      &event_bus,
      batch_size,
      compact_threshold,
      compact_min_size,
    ) else {
      return worker.release_tables(&release_queue, &mut steps);
    };
    if !matches!(err, Error::IO(_) | Error::WALFailed(_)) {
      panic!("{err}");
    }
    error!("gc failed: {err}");
  }
}
