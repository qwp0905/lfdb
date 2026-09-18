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
  background::{OnceThread, PendingTask, ThreadPool},
  blob::{BlobAppendGuard, BlobId, BlobLen, BlobOffset, BlobStorage},
  btree::{BTreeIndex, MergeSortable, ReadonlyPolicy, WritablePolicy},
  cache::BlockCache,
  disk::{AlignedBuf, AtomicDiskPointer, Pointer},
  mvcc::VersionController,
  objects::{BTreeNodeView, DataEntryView, StaticKey, TreeHeader, HEADER_POINTER},
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
    self.version_controller.is_aborted(&owner)
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
    slot: &mut crate::cache::RefedSlot,
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
  ) -> Result<crate::cache::CachedSlot<'_>> {
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

struct TableRecovery {
  block_cache: Arc<BlockCache>,
  recorder: Arc<PageRecorder>,
  table: TableHandleRef,
  status: Arc<RecoveryStatus>,
  pool: Arc<OnceThread<ThreadPool>>,
}
impl Clone for TableRecovery {
  fn clone(&self) -> Self {
    Self {
      block_cache: self.block_cache.clone(),
      recorder: self.recorder.clone(),
      table: self.table.clone(),
      status: self.status.clone(),
      pool: self.pool.clone(),
    }
  }
}
impl TableRecovery {
  const fn new(
    block_cache: Arc<BlockCache>,
    recorder: Arc<PageRecorder>,
    table: TableHandleRef,
    status: Arc<RecoveryStatus>,
    pool: Arc<OnceThread<ThreadPool>>,
  ) -> Self {
    Self {
      block_cache,
      recorder,
      table,
      status,
      pool,
    }
  }

  fn fetch_job(
    &self,
    closure: impl FnOnce() -> Result<RecoveryResult> + Send + 'static,
  ) -> PendingTask<Result<RecoveryResult>> {
    self.status.inc_remaining();
    self.pool.spawn(closure)
  }

  fn complete_job(&self) -> Option<PendingTask<Result<RecoveryResult>>> {
    if self.status.dec_remaining() > 1 {
      return None;
    }
    Some(self.fetch_job(self.complete_table()))
  }

  fn scan_node_internal(&self, ptr: Pointer, level: u16) -> Result<RecoveryResult> {
    if !self.status.set_visited(ptr) {
      return Ok(RecoveryResult(self.complete_job().map(|r| vec![r])));
    };
    self.status.set_max_used(ptr);

    let mut pendings = Vec::new();

    match self
      .block_cache
      .read(ptr, &self.table)?
      .for_read()
      .as_ref()
      .view::<BTreeNodeView>()?
    {
      BTreeNodeView::Internal(node) => {
        if let Some((k, p)) = node.get_right() {
          pendings.push(self.fetch_job(self.scan_node(p, level)));
          self.status.set_half_split(p, Some(k.to_vec()), level);
        }
        for c in node.get_all_child()? {
          pendings.push(self.fetch_job(self.scan_node(c, level - 1)));
          self.status.set_child_reachable(c);
        }
      }
      BTreeNodeView::Leaf(node) => {
        if let Some(p) = node.get_next() {
          pendings.push(self.fetch_job(self.scan_node(p, level)));
          self.status.set_half_split(p, None, level);
        }
        let mut iter = node.get_entries()?;
        while let Some(e) = iter.try_next()? {
          if let Some(p) = e.next {
            pendings.push(self.fetch_job(self.scan_entry(p)));
          }
        }
      }
    };

    if let Some(pending) = self.complete_job() {
      pendings.push(pending);
    }
    Ok(RecoveryResult(Some(pendings)))
  }

  fn scan_node(
    &self,
    ptr: Pointer,
    level: u16,
  ) -> impl FnOnce() -> Result<RecoveryResult> + Send {
    let this = self.clone();
    move || this.scan_node_internal(ptr, level)
  }

  fn scan_entry_internal(&self, ptr: Pointer) -> Result<RecoveryResult> {
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
    Ok(RecoveryResult(self.complete_job().map(|r| vec![r])))
  }

  fn scan_entry(&self, ptr: Pointer) -> impl FnOnce() -> Result<RecoveryResult> + Send {
    let this = self.clone();
    move || this.scan_entry_internal(ptr)
  }

  fn complete_table_internal(&self) -> Result<RecoveryResult> {
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
    if half_split.is_empty() {
      return Ok(RecoveryResult(None));
    }

    info!("{} half split detected at table {name}", half_split.len());

    let mut pending = Vec::with_capacity(half_split.len());
    for (split_ptr, split_key, level) in half_split {
      let task = self.recovery_split(split_key, split_ptr, level);
      pending.push(self.pool.spawn(task));
    }
    Ok(RecoveryResult(Some(pending)))
  }

  fn recovery_split(
    &self,
    split_key: Option<StaticKey>,
    split_ptr: Pointer,
    level: u16,
  ) -> impl FnOnce() -> Result<RecoveryResult> {
    let this = self.clone();
    move || {
      this
        .recovery_split_internal(split_key, split_ptr, level)
        .map(|_| RecoveryResult(None))
    }
  }

  fn recovery_split_internal(
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

  fn complete_table(&self) -> impl FnOnce() -> Result<RecoveryResult> {
    let this = self.clone();
    move || this.complete_table_internal()
  }
}

pub fn recovery(
  block_cache: Arc<BlockCache>,
  recorder: Arc<PageRecorder>,
  tables: &TableMapper,
  max_used: HashMap<TableId, Pointer>,
  pool: Arc<OnceThread<ThreadPool>>,
) -> Result {
  let open_handles = tables.get_all();

  let mut pending = ChunkQueue::new();
  for table in open_handles {
    let task = pool.spawn(create_tasks(
      block_cache.clone(),
      recorder.clone(),
      pool.clone(),
      table,
      &max_used,
    ));
    pending.push(task);
  }

  while let Some(task) = pending.pop() {
    pending.extend(task.wait().unwrap()?.flatten());
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
    slot: &mut crate::cache::RefedSlot,
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
  ) -> Result<crate::cache::CachedSlot<'_>> {
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
  fn inc_remaining(&self) {
    self.remaining.fetch_add(1, Ordering::Release);
  }
  fn dec_remaining(&self) -> usize {
    self.remaining.fetch_sub(1, Ordering::AcqRel)
  }
}
unsafe impl Sync for RecoveryStatus {}

struct RecoveryResult(Option<Vec<PendingTask<Result<RecoveryResult>>>>);
impl RecoveryResult {
  fn flatten(self) -> impl Iterator<Item = PendingTask<Result<Self>>> {
    self.0.into_iter().flatten()
  }
}

fn create_tasks(
  block_cache: Arc<BlockCache>,
  recorder: Arc<PageRecorder>,
  thread_pool: Arc<OnceThread<ThreadPool>>,
  table: TableHandleRef,
  max_used: &HashMap<TableId, Pointer>,
) -> impl FnOnce() -> Result<RecoveryResult> + Send {
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

    let recovery = TableRecovery::new(
      block_cache,
      recorder,
      table,
      Arc::new(status),
      thread_pool.clone(),
    );

    let pending = thread_pool.spawn(recovery.scan_node(root, height));
    Ok(RecoveryResult(Some(vec![pending])))
  }
}
