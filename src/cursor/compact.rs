use std::{
  cell::Cell,
  collections::{LinkedList, VecDeque},
  mem::take,
  ops::Bound,
  sync::Arc,
  time::Duration,
};

use crossbeam::queue::SegQueue;

use super::{
  BTreeIndex, CreatablePolicy, DropTableCommitted, ReadonlyPolicy, WritablePolicy,
};
use crate::{
  background::{BackgroundThread, EventBus, OwnedSubscription, WorkBuilder},
  binding_events,
  cache::{BlockCache, RefedSlot},
  disk::Pointer,
  info,
  serialize::Serializable,
  table::{PinnedHandle, TableHandleRef, TableMapper, TableMetadata},
  trace,
  transaction::{PageRecorder, TxSnapshot, TxState, VersionVisibility},
  utils::{ToArc, ToBox},
  wal::{TxId, RESERVED_TX, WAL},
  warn, Result,
};

struct MiniTx<'a> {
  state: TxState<'a>,
  snapshot: TxSnapshot<'a>,
  block_cache: &'a BlockCache,
  version_visibility: &'a VersionVisibility,
  recorder: &'a PageRecorder,
  wal: &'a WAL,
  committed: Cell<bool>,
  modified: Cell<bool>,
}
impl<'a> MiniTx<'a> {
  fn start(
    version_visibility: &'a VersionVisibility,
    wal: &'a WAL,
    block_cache: &'a BlockCache,
    recorder: &'a PageRecorder,
  ) -> Result<Self> {
    let (state, snapshot) = version_visibility.new_transaction();
    wal.append_start(state.get_id())?;
    Ok(Self {
      state,
      snapshot,
      block_cache,
      recorder,
      version_visibility,
      wal,
      committed: Cell::new(false),
      modified: Cell::new(false),
    })
  }

  fn abort(&mut self) -> Result {
    if self.committed.get() {
      return Ok(());
    }

    if self.modified.get() {
      self.wal.append_abort(self.state.get_id())?;
      self.version_visibility.set_abort(self.state.get_id());
    }
    self.committed.set(true);
    self.state.deactive();
    Ok(())
  }

  fn commit(&mut self) -> Result {
    if self.committed.get() {
      return Ok(());
    }
    if self.modified.get() {
      self.wal.commit_and_flush(self.state.get_id())?;
    }
    self.committed.set(true);
    self.state.deactive();
    Ok(())
  }
}
impl<'a> Drop for MiniTx<'a> {
  fn drop(&mut self) {
    let _ = self.abort();
  }
}

impl<'a> ReadonlyPolicy for &MiniTx<'a> {
  fn fetch_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.block_cache.read(pointer, table)
  }
  fn is_aborted(&self, owner: TxId) -> bool {
    self.snapshot.is_aborted(&owner)
  }
  fn is_owned(&self, owner: TxId) -> bool {
    self.state.get_id() == owner
  }
  fn is_readable(&self, version: TxId) -> bool {
    version <= self.state.get_id()
  }
  fn is_active(&self, owner: TxId) -> bool {
    self.snapshot.is_active(&owner)
  }
}
impl<'a> WritablePolicy for &MiniTx<'a> {
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut RefedSlot,
    data: &T,
    table: &TableHandleRef,
  ) -> Result {
    self
      .recorder
      .serialize_and_log(self.state.get_id(), table.get_id(), slot, data)?;
    self.modified.set(true);
    Ok(())
  }

  fn alloc_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.block_cache.alloc(pointer, &table)
  }
}
impl<'a> CreatablePolicy for &MiniTx<'a> {
  fn current_owner(&self) -> TxId {
    self.state.get_id()
  }
  fn current_version(&self) -> TxId {
    self.version_visibility.current_version()
  }
  fn wait_close(&self, _owner: TxId) {}
}

struct CompactionReadPolicy<'a> {
  block_cache: &'a BlockCache,
  version_visibility: &'a VersionVisibility,
}
impl<'a> ReadonlyPolicy for CompactionReadPolicy<'a> {
  fn is_aborted(&self, owner: TxId) -> bool {
    self.version_visibility.is_aborted(&owner)
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
}

struct CompactionWritePolicy<'a> {
  block_cache: &'a BlockCache,
  version_visibility: &'a VersionVisibility,
  recorder: &'a PageRecorder,
}
impl<'a> ReadonlyPolicy for CompactionWritePolicy<'a> {
  fn fetch_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.block_cache.read(pointer, table)
  }
  fn is_aborted(&self, owner: TxId) -> bool {
    self.version_visibility.is_aborted(&owner)
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
}
impl<'a> WritablePolicy for CompactionWritePolicy<'a> {
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut RefedSlot,
    data: &T,
    table: &TableHandleRef,
  ) -> Result {
    self
      .recorder
      .serialize_and_log(RESERVED_TX, table.get_id(), slot, data)
  }

  fn alloc_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.block_cache.alloc(pointer, table)
  }
}

pub const COMPACTION_INTERVAL: Duration = Duration::from_secs(1);

fn wait_compaction(
  queue: Arc<SegQueue<CompactTask>>,
  tables: Arc<TableMapper>,
  block_cache: Arc<BlockCache>,
  versions: Arc<VersionVisibility>,
  wal: Arc<WAL>,
  recorder: Arc<PageRecorder>,
  event_bus: Arc<EventBus>,
) -> impl FnMut(Option<()>) -> Result {
  let meta_table = tables.meta_table();
  let mut triggered = LinkedList::new();
  let mut waited = VecDeque::new();

  move |_| {
    while let Some(task) = queue.pop() {
      match task {
        CompactTask::Committed(committed) => triggered.push_back((
          committed.old,
          committed.new,
          committed.metadata,
          committed.commit_version,
        )),
        CompactTask::New(trigger) => {
          let old = trigger.old;
          let table_name = old.get_name();
          let (new_table, wait_until, metadata) = {
            let mut tx = MiniTx::start(&versions, &wal, &block_cache, &recorder)?;

            let index = BTreeIndex::new(&tx);

            let mut metadata =
              match index.get(table_name.as_bytes(), &meta_table)?.flatten() {
                Some(bytes) => TableMetadata::from_bytes(&bytes)?,
                None => continue,
              };

            if metadata.get_compaction_id().is_some() {
              trace!("table {table_name} compacting skipped since already compacted.");
              continue;
            }

            info!("table {table_name} compacting triggered.");
            let table_meta = tables.create_metadata(table_name);
            metadata.set_compaction(&table_meta);

            index.insert_if_matched(
              table_name.as_bytes(),
              Some(metadata.to_vec()),
              &meta_table,
            )?;

            let new_table = tables.create_handle(&table_meta)?.try_pin().unwrap();

            tables.insert(new_table.handle().clone());

            index.initialize(new_table.handle())?;

            tx.commit()?;
            (new_table, versions.current_version(), table_meta)
          };

          info!("table {table_name} compacting wait until another tx close.");
          triggered.push_back((old.clone(), new_table, metadata, wait_until));
        }
      }
    }

    let min_version = versions.min_version();
    for (old, new, metadata, _) in triggered.extract_if(|(_, _, _, v)| min_version >= *v)
    {
      waited.push_back((old, new, metadata));
    }

    for (old, new, metadata) in take(&mut waited) {
      match old.try_pin() {
        Some(old) => event_bus.publish(CompactionPublished::new(old, new, metadata)),
        None => continue,
      }
    }

    Ok(())
  }
}

pub fn handle_compaction(
  tables: Arc<TableMapper>,
  block_cache: Arc<BlockCache>,
  versions: Arc<VersionVisibility>,
  wal: Arc<WAL>,
  recorder: Arc<PageRecorder>,
  event_bus: Arc<EventBus>,
) -> impl Fn(CompactionPublished) -> Result {
  let meta_table = tables.meta_table();
  move |task| {
    do_compaction(
      &block_cache,
      &versions,
      &wal,
      &recorder,
      &meta_table,
      task.old,
      task.new,
      task.metadata,
      &event_bus,
    )
  }
}

fn do_compaction(
  block_cache: &BlockCache,
  version_visibility: &VersionVisibility,
  wal: &WAL,
  recorder: &PageRecorder,
  meta_table: &TableHandleRef,
  old_table: PinnedHandle,
  new: PinnedHandle,
  new_metadata: TableMetadata,
  event_bus: &EventBus,
) -> Result {
  let table_name = old_table.get_name();
  info!("table {table_name} compacting begin.");
  let mut moved_count = 0;

  {
    let old_index = BTreeIndex::new(CompactionReadPolicy {
      block_cache,
      version_visibility,
    });

    let mut old_snapshot =
      old_index.scan(old_table.handle(), &Bound::Unbounded, &Bound::Unbounded)?;

    let new_index = BTreeIndex::new(CompactionWritePolicy {
      block_cache,
      version_visibility,
      recorder,
    });

    'compaction: loop {
      for _ in 0..100 {
        match old_snapshot.next_snapshot()? {
          Some(snap) => {
            new_index.apply_snapshot(snap, new.handle())?;
            moved_count += 1;
          }
          None => break 'compaction,
        }
      }

      let tx = MiniTx::start(version_visibility, wal, block_cache, recorder)?;
      if !BTreeIndex::new(&tx).contains(table_name.as_bytes(), meta_table)? {
        warn!("table {table_name} already dropped.");
        return Ok(());
      }
    }
  }

  info!("table {table_name} compacting copied {moved_count} count record complete.");

  let (tx_id, version) = {
    let mut tx = MiniTx::start(version_visibility, wal, block_cache, recorder)?;
    let index = BTreeIndex::new(&tx);

    if !index.contains(table_name.as_bytes(), meta_table)? {
      warn!("table {table_name} already dropped.");
      return Ok(());
    }

    index.insert_if_matched(
      table_name.as_bytes(),
      Some(new_metadata.to_vec()),
      &meta_table,
    )?;

    tx.commit()?;
    (tx.state.get_id(), version_visibility.current_version())
  };

  info!("table {table_name} compacting totally complete.");
  event_bus.publish(CompactionCompleted {
    old: old_table,
    _new: new,
    owner: tx_id,
    commit_version: version,
  });

  Ok(())
}

const fn after_compaction(
  event_bus: Arc<EventBus>,
  version_visibility: Arc<VersionVisibility>,
) -> impl FnMut(Option<CompactionCompleted>) {
  let mut buffered = LinkedList::new();
  move |data| {
    if let Some(v) = data {
      buffered.push_back(v);
    }

    let min_version = version_visibility.min_version();
    for task in buffered.extract_if(|v| min_version >= v.commit_version) {
      event_bus.publish(DropTableCommitted::new(
        task.old.into_inner(),
        task.owner,
        task.commit_version,
      ));
    }
  }
}

pub struct CompactionCommitted {
  old: TableHandleRef,
  new: PinnedHandle,
  metadata: TableMetadata,
  commit_version: TxId,
}
impl CompactionCommitted {
  pub const fn new(
    old: TableHandleRef,
    new: PinnedHandle,
    metadata: TableMetadata,
    commit_version: TxId,
  ) -> Self {
    Self {
      old,
      new,
      metadata,
      commit_version,
    }
  }
}

enum CompactTask {
  Committed(CompactionCommitted),
  New(CompactionTriggered),
}

pub struct CompactionTriggered {
  old: TableHandleRef,
}
impl CompactionTriggered {
  pub const fn new(old: TableHandleRef) -> Self {
    Self { old }
  }
}
pub struct CompactionPublished {
  old: PinnedHandle,
  new: PinnedHandle,
  metadata: TableMetadata,
}
impl CompactionPublished {
  pub const fn new(
    old: PinnedHandle,
    new: PinnedHandle,
    metadata: TableMetadata,
  ) -> Self {
    Self { old, new, metadata }
  }
}
struct CompactionCompleted {
  old: PinnedHandle,
  _new: PinnedHandle,
  owner: TxId,
  commit_version: TxId,
}

pub struct Compactor {
  queue: Arc<SegQueue<CompactTask>>,
  wait_compaction: Box<dyn BackgroundThread<(), Result>>,
  do_compaction: Arc<dyn BackgroundThread<CompactionPublished, Result>>,
  after_compaction: Arc<dyn BackgroundThread<CompactionCompleted>>,
}
impl Compactor {
  pub fn new(
    block_cache: Arc<BlockCache>,
    tables: Arc<TableMapper>,
    recorder: Arc<PageRecorder>,
    version_visibility: Arc<VersionVisibility>,
    wal: Arc<WAL>,
    event_bus: Arc<EventBus>,
  ) -> Arc<Self> {
    let after_compaction = WorkBuilder::new()
      .name("tree after compaction")
      .single()
      .interval(
        COMPACTION_INTERVAL,
        after_compaction(event_bus.clone(), version_visibility.clone()),
      )
      .to_arc();

    let do_compaction = WorkBuilder::new()
      .name("tree compaction")
      .multi(1)
      .shared(handle_compaction(
        tables.clone(),
        block_cache.clone(),
        version_visibility.clone(),
        wal.clone(),
        recorder.clone(),
        event_bus.clone(),
      ))
      .to_arc();

    let queue = SegQueue::new().to_arc();
    let wait_compaction = WorkBuilder::new()
      .name("tree waiting compaction")
      .single()
      .interval(
        COMPACTION_INTERVAL,
        wait_compaction(
          queue.clone(),
          tables.clone(),
          block_cache.clone(),
          version_visibility.clone(),
          wal.clone(),
          recorder.clone(),
          event_bus.clone(),
        ),
      )
      .to_box();

    let this = Arc::new(Self {
      queue,
      wait_compaction,
      do_compaction,
      after_compaction,
    });
    event_bus.register(&this.after_compaction);
    event_bus.register(&this.do_compaction);
    event_bus.register(&this);
    this
  }

  pub fn close(&self) {
    self.wait_compaction.close();
    self.do_compaction.close();
    self.after_compaction.close();
  }
}

impl OwnedSubscription<CompactionCommitted> for Compactor {
  fn handle(&self, event: CompactionCommitted) {
    self.queue.push(CompactTask::Committed(event));
  }
}
impl OwnedSubscription<CompactionTriggered> for Compactor {
  fn handle(&self, event: CompactionTriggered) {
    self.queue.push(CompactTask::New(event))
  }
}
binding_events!(Compactor {
  owned: [CompactionCommitted, CompactionTriggered]
});
