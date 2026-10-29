use std::{
  cell::UnsafeCell,
  collections::{HashMap, HashSet},
  mem::take,
  ops::Bound,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
  },
};

use crossbeam_skiplist::{SkipMap, SkipSet};

use crate::{
  background::{PendingTask, ThreadPool},
  blob::{BlobAppendGuard, BlobId, BlobLen, BlobOffset, BlobStorage},
  btree::{BTreeIndex, MergeSortable, ReadonlyPolicy, WritablePolicy},
  cache::BlockCache,
  disk::{AtomicDiskPointer, Pointer},
  mvcc::VersionController,
  objects::{BTreeNodeView, DataEntryView, StaticKey, TreeHeader, HEADER_POINTER},
  page::AlignedBuf,
  table::{TableHandleRef, TableId, TableMapper, TableMetadata},
  transaction::PageRecorder,
  utils::{debug, info, ChunkQueue, ShortenedMutex},
  wal::{TxId, RESERVED_TX},
  Result,
};

struct TableOpenPolicy<'a, R> {
  block_cache: &'a BlockCache,
  version_controller: &'a VersionController,
  blob: &'a BlobStorage,
  recorder: R,
}
impl<'a, R> ReadonlyPolicy for TableOpenPolicy<'a, R> {
  fn is_aborted(&self, owner: TxId) -> bool {
    self.version_controller.is_aborted(owner)
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
    self.block_cache.read(pointer, table)
  }
  fn read_blob(
    &self,
    blob_id: BlobId,
    offset: BlobOffset,
    len: BlobLen,
  ) -> Result<AlignedBuf> {
    let blob = self
      .blob
      .get(blob_id)
      .unwrap_or_else(|| unreachable!("blob id {blob_id} must exists"));
    blob.read_at(offset, len)
  }
}

impl<'a> WritablePolicy for TableOpenPolicy<'a, &'a PageRecorder> {
  fn serialize_and_log<T: crate::objects::Serializable>(
    &self,
    slot: &mut crate::cache::PreparedSlot,
    data: &T,
    table: &TableHandleRef,
  ) -> Result {
    self
      .recorder
      .serialize_and_log(RESERVED_TX, table.get_id(), RESERVED_TX, slot, data)
  }

  fn alloc_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<crate::cache::AllocatedSlot<'_>> {
    self.block_cache.alloc(pointer, table)
  }

  fn write_blob(&self, data: Vec<u8>) -> Result<BlobAppendGuard<'_>> {
    self.blob.append(data)
  }
}

pub fn initialize(
  block_cache: &BlockCache,
  tables: &TableMapper,
  recorder: &PageRecorder,
  version_controller: &VersionController,
  blob: &BlobStorage,
) -> Result {
  let policy = TableOpenPolicy {
    block_cache,
    version_controller,
    recorder,
    blob,
  };
  BTreeIndex::new(policy).initialize(&tables.meta_table())?;
  Ok(())
}

pub struct OpenTablesResult {
  pub handles: Vec<(TableHandleRef, TableMetadata)>,
  pub in_compaction: Vec<(
    (TableHandleRef, TableMetadata),
    (TableHandleRef, TableMetadata),
  )>,
}
pub fn open_tables(
  block_cache: &BlockCache,
  tables: &TableMapper,
  version_controller: &VersionController,
  blob: &BlobStorage,
) -> Result<OpenTablesResult> {
  let mut handles = vec![];
  let mut in_compaction = vec![];
  let meta_table = tables.meta_table();

  let index = BTreeIndex::new(TableOpenPolicy {
    block_cache,
    version_controller,
    blob,
    recorder: (),
  });

  let mut iter = index.range(&meta_table, &Bound::Unbounded, &Bound::Unbounded)?;

  while let Some((_, bytes)) = iter.get_next_pair()? {
    let metadata = TableMetadata::from_bytes(&bytes)?;
    match metadata.get_compaction_metadata() {
      Some(c_meta) => in_compaction.push((
        (tables.create_handle(&metadata)?, metadata),
        (tables.create_handle(&c_meta)?, c_meta),
      )),
      None => handles.push((tables.create_handle(&metadata)?, metadata)),
    }
  }

  Ok(OpenTablesResult {
    handles,
    in_compaction,
  })
}

enum RecoveryJob {
  ScanNode(Pointer, u16),
  ScanEntry(Pointer),
  CompleteTable,
  RecoverySplit(Option<StaticKey>, Pointer, u16),
}

#[derive(Clone)]
struct TableRecovery {
  block_cache: Arc<BlockCache>,
  recorder: Arc<PageRecorder>,
  table: TableHandleRef,
  status: Arc<RecoveryStatus>,
}
impl TableRecovery {
  const fn new(
    block_cache: Arc<BlockCache>,
    recorder: Arc<PageRecorder>,
    table: TableHandleRef,
    status: Arc<RecoveryStatus>,
  ) -> Self {
    Self {
      block_cache,
      recorder,
      table,
      status,
    }
  }

  fn run(&self, job: RecoveryJob) -> Result<Vec<RecoveryJob>> {
    match job {
      RecoveryJob::ScanNode(ptr, level) => self.scan_node(ptr, level),
      RecoveryJob::ScanEntry(ptr) => self.scan_entry(ptr),
      RecoveryJob::CompleteTable => self.complete_table(),
      RecoveryJob::RecoverySplit(key, ptr, level) => {
        self.recovery_split(key, ptr, level).map(|_| Vec::new())
      }
    }
  }

  fn complete_job(&self) -> Option<RecoveryJob> {
    if self.status.dec_remaining() > 1 {
      return None;
    }
    Some(RecoveryJob::CompleteTable)
  }

  fn scan_node(&self, ptr: Pointer, level: u16) -> Result<Vec<RecoveryJob>> {
    if !self.status.set_visited(ptr) {
      return Ok(self.complete_job().into_iter().collect());
    };
    self.status.set_max_used(ptr);

    let mut jobs = Vec::new();

    match self
      .block_cache
      .read(ptr, &self.table)?
      .for_read()
      .as_ref()
      .view::<BTreeNodeView>()?
    {
      BTreeNodeView::Internal(node) => {
        if let Some((k, p)) = node.get_right() {
          jobs.push(RecoveryJob::ScanNode(p, level));
          self.status.set_half_split(p, Some(k.to_vec()), level);
        }
        for c in node.get_all_child()? {
          jobs.push(RecoveryJob::ScanNode(c, level - 1));
          self.status.set_child_reachable(c);
        }
      }
      BTreeNodeView::Leaf(node) => {
        if let Some(p) = node.get_next() {
          jobs.push(RecoveryJob::ScanNode(p, level));
          self.status.set_half_split(p, None, level);
        }
        let mut iter = node.get_entries();
        while let Some(e) = iter.try_next()? {
          if let Some(p) = e.next {
            jobs.push(RecoveryJob::ScanEntry(p));
          }
        }
      }
    };

    // Count the children before releasing this job, so the table cannot look
    // complete while they are still outstanding.
    self.status.add_remaining(jobs.len());
    jobs.extend(self.complete_job());
    Ok(jobs)
  }

  fn scan_entry(&self, ptr: Pointer) -> Result<Vec<RecoveryJob>> {
    let mut next = Some(ptr);
    while let Some(ptr) = next.take() {
      if !self.status.set_visited(ptr) {
        continue;
      }
      self.status.set_max_used(ptr);
      next = self
        .block_cache
        .read(ptr, &self.table)?
        .for_read()
        .as_ref()
        .view::<DataEntryView>()?
        .get_next();
    }
    Ok(self.complete_job().into_iter().collect())
  }

  fn complete_table(&self) -> Result<Vec<RecoveryJob>> {
    let name = self.table.get_name();
    debug!("table {name} completed to collect orphaned blocks.",);

    let end = self.status.get_max_used();
    let mut visited = self.status.take_visited();
    (0..=end)
      .filter(|i| !visited.remove(i))
      .for_each(|i| self.table.free().dealloc(i));
    self.table.free().replay(end + 1);

    let half_split = self
      .status
      .take_half_split()
      .into_iter()
      .filter(|(p, _)| !self.status.is_child_reachable(p))
      .map(|(p, (k, l))| (p, k, l))
      .collect::<Vec<_>>();
    if !half_split.is_empty() {
      info!("{} half split detected at table {name}", half_split.len());
    }

    Ok(
      half_split
        .into_iter()
        .map(|(ptr, key, level)| RecoveryJob::RecoverySplit(key, ptr, level))
        .collect(),
    )
  }

  fn recovery_split(
    &self,
    split_key: Option<StaticKey>,
    split_ptr: Pointer,
    level: u16,
  ) -> Result {
    let index = BTreeIndex::new(RecoveryPolicy {
      block_cache: &self.block_cache,
      recorder: &self.recorder,
    });

    if let Some(k) = split_key {
      index.recovery_half_split(k, split_ptr, level, &self.table)?;
      return Ok(());
    }

    let slot = self.block_cache.read(split_ptr, &self.table)?.for_read();
    let node = slot.as_ref().view::<BTreeNodeView>()?.into_leaf()?;

    let key = node.top()?.to_vec();
    index.recovery_half_split(key, split_ptr, level, &self.table)?;
    Ok(())
  }
}

fn spawn_job(
  pool: &ThreadPool,
  recovery: &TableRecovery,
  job: RecoveryJob,
) -> (TableRecovery, PendingTask<Result<Vec<RecoveryJob>>>) {
  let this = recovery.clone();
  (recovery.clone(), pool.spawn(move || this.run(job)))
}

pub fn recovery(
  block_cache: Arc<BlockCache>,
  recorder: Arc<PageRecorder>,
  tables: &TableMapper,
  max_used: HashMap<TableId, Pointer>,
  pool: &ThreadPool,
) -> Result {
  let initializing = tables
    .get_all()
    .into_iter()
    .map(|table| {
      pool.spawn(init_table(
        block_cache.clone(),
        recorder.clone(),
        table,
        &max_used,
      ))
    })
    .collect::<Vec<_>>();

  let mut pending = ChunkQueue::new();
  for task in initializing {
    let (recovery, job) = task.wait().unwrap()?;
    pending.push(spawn_job(pool, &recovery, job));
  }

  while let Some((recovery, task)) = pending.pop() {
    for job in task.wait().unwrap()? {
      pending.push(spawn_job(pool, &recovery, job));
    }
  }

  info!("orphaned block has released successfully.");
  Ok(())
}

struct RecoveryPolicy<'a> {
  block_cache: &'a BlockCache,
  recorder: &'a PageRecorder,
}
impl<'a> ReadonlyPolicy for RecoveryPolicy<'a> {
  fn is_aborted(&self, _: TxId) -> bool {
    unreachable!()
  }
  fn is_owned(&self, _: TxId) -> bool {
    unreachable!()
  }
  fn is_readable(&self, _: TxId) -> bool {
    unreachable!()
  }
  fn is_active(&self, _: TxId) -> bool {
    unreachable!()
  }
  fn fetch_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.block_cache.read(pointer, table)
  }
  fn read_blob(&self, _: BlobId, _: BlobOffset, _: BlobLen) -> Result<AlignedBuf> {
    unreachable!()
  }
}
impl<'a> WritablePolicy for RecoveryPolicy<'a> {
  fn write_blob(&self, _: Vec<u8>) -> Result<BlobAppendGuard<'_>> {
    unreachable!()
  }
  fn serialize_and_log<T: crate::objects::Serializable>(
    &self,
    slot: &mut crate::cache::PreparedSlot,
    data: &T,
    table: &TableHandleRef,
  ) -> Result {
    self
      .recorder
      .serialize_and_log(RESERVED_TX, table.get_id(), RESERVED_TX, slot, data)
  }
  fn alloc_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<crate::cache::AllocatedSlot<'_>> {
    self.block_cache.alloc(pointer, table)
  }
}

type HalfSplit = SkipMap<Pointer, (Option<StaticKey>, u16)>;
struct RecoveryStatus {
  visited: Mutex<HashSet<Pointer>>,
  half_split: UnsafeCell<Option<HalfSplit>>,
  child_reachable: SkipSet<Pointer>,
  max_used: AtomicDiskPointer,
  remaining: AtomicUsize,
}
impl RecoveryStatus {
  fn take_half_split(&self) -> HalfSplit {
    unsafe {
      (*self.half_split.get())
        .take()
        .unwrap_or_else(|| unreachable!())
    }
  }

  fn set_half_split(&self, ptr: Pointer, right_key: Option<StaticKey>, level: u16) {
    let half_split = unsafe {
      (*self.half_split.get())
        .as_ref()
        .unwrap_or_else(|| unreachable!())
    };
    half_split.insert(ptr, (right_key, level));
  }

  fn set_visited(&self, ptr: Pointer) -> bool {
    self.visited.l().insert(ptr)
  }
  fn take_visited(&self) -> HashSet<Pointer> {
    take(&mut self.visited.l())
  }
  fn set_max_used(&self, ptr: Pointer) {
    self.max_used.fetch_max(ptr, Ordering::Release);
  }
  fn get_max_used(&self) -> Pointer {
    self.max_used.load(Ordering::Acquire)
  }

  fn set_child_reachable(&self, ptr: Pointer) {
    self.child_reachable.insert(ptr);
  }
  fn is_child_reachable(&self, ptr: &Pointer) -> bool {
    self.child_reachable.contains(ptr)
  }
  fn add_remaining(&self, count: usize) {
    self.remaining.fetch_add(count, Ordering::Release);
  }
  fn dec_remaining(&self) -> usize {
    self.remaining.fetch_sub(1, Ordering::AcqRel)
  }
}
unsafe impl Sync for RecoveryStatus {}

fn init_table(
  block_cache: Arc<BlockCache>,
  recorder: Arc<PageRecorder>,
  table: TableHandleRef,
  max_used: &HashMap<TableId, Pointer>,
) -> impl FnOnce() -> Result<(TableRecovery, RecoveryJob)> + Send {
  let max_used = max_used
    .get(&table.get_id())
    .copied()
    .unwrap_or(HEADER_POINTER);
  move || {
    debug!(
      "table {} start to collect orphaned blocks.",
      table.get_name()
    );

    let (root, height) = {
      let header = block_cache
        .read(HEADER_POINTER, &table)?
        .for_read()
        .as_ref()
        .deserialize::<TreeHeader>()?;
      (header.get_root(), header.get_height())
    };

    let status = RecoveryStatus {
      visited: Mutex::new(HashSet::from_iter([HEADER_POINTER])),
      half_split: UnsafeCell::new(Some(SkipMap::new())),
      child_reachable: SkipSet::new(),
      max_used: AtomicDiskPointer::new(max_used),
      remaining: AtomicUsize::new(1),
    };

    let recovery = TableRecovery::new(block_cache, recorder, table, Arc::new(status));
    Ok((recovery, RecoveryJob::ScanNode(root, height)))
  }
}
