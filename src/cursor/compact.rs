use std::{
  cell::Cell,
  collections::{LinkedList, VecDeque},
  mem::take,
  sync::Arc,
  time::Duration,
};

use crossbeam::{atomic::AtomicCell, queue::SegQueue};

use super::{
  BTreeIndex, CreatablePolicy, DropTableCommitted, ReadonlyPolicy, Snapshotter,
  WritablePolicy,
};
use crate::{
  background::{
    BackgroundThread, EventBus, OwnedSubscription, SharedSubscription, WorkBuilder,
  },
  binding_events,
  cache::{BlockCache, RefedSlot},
  disk::Pointer,
  error, info,
  serialize::Serializable,
  table::{PinnedHandle, TableHandleRef, TableMapper, TableMetadata, TableName},
  trace,
  transaction::{PageRecorder, TxSnapshot, TxState, VersionVisibility},
  utils::{ToArc, ToBox, UnsafeBorrowMut},
  wal::{TxId, WALFailed, RESERVED_TX, WAL},
  warn, Error, Result,
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
    let Some((snapshot, state)) = version_visibility.new_transaction() else {
      return Err(Error::EngineUnavailable);
    };
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

impl<'a> ReadonlyPolicy for MiniTx<'a> {
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
impl<'a> WritablePolicy for MiniTx<'a> {
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut RefedSlot,
    data: &T,
    table: &TableHandleRef,
  ) -> Result {
    self.recorder.serialize_and_log(
      self.state.get_id(),
      table.get_id(),
      self.current_version(),
      slot,
      data,
    )?;
    self.modified.set(true);
    Ok(())
  }

  fn alloc_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.block_cache.alloc(pointer, table)
  }
}
impl<'a> CreatablePolicy for MiniTx<'a> {
  fn current_owner(&self) -> TxId {
    self.state.get_id()
  }
  fn current_version(&self) -> TxId {
    self.state.current_version()
  }
  fn wait_close(&self, _owner: TxId) {}
}

struct CompactionReadPolicy {
  block_cache: Arc<BlockCache>,
  version_visibility: Arc<VersionVisibility>,
}
impl ReadonlyPolicy for Arc<CompactionReadPolicy> {
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

struct CompactionWritePolicy {
  block_cache: Arc<BlockCache>,
  version_visibility: Arc<VersionVisibility>,
  recorder: Arc<PageRecorder>,
}
impl ReadonlyPolicy for CompactionWritePolicy {
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
impl WritablePolicy for CompactionWritePolicy {
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut RefedSlot,
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

const COMPACTION_INTERVAL: Duration = Duration::from_secs(1);

fn create_compaction(
  version_visibility: &VersionVisibility,
  wal: &WAL,
  block_cache: &BlockCache,
  recorder: &PageRecorder,
  tables: &TableMapper,
  meta_table: &TableHandleRef,
  table_name: &TableName,
) -> Result<Option<(PinnedHandle, TxId, TableMetadata)>> {
  let mut tx = MiniTx::start(version_visibility, wal, block_cache, recorder)?;
  let index = BTreeIndex::new(&tx);

  let Some(bytes) = index.get(table_name.as_bytes(), meta_table)?.flatten() else {
    return Ok(None);
  };

  let mut metadata = TableMetadata::from_bytes(&bytes)?;
  if metadata.get_compaction_id().is_some() {
    trace!("table {table_name} compacting skipped since already compacted.");
    return Ok(None);
  }

  info!("table {table_name} compacting triggered.");
  let table_meta = tables.create_metadata(table_name);
  metadata.set_compaction(&table_meta);

  if let Err(err) =
    index.insert_if_matched(table_name.as_bytes(), Some(metadata.to_vec()), meta_table)
  {
    if matches!(err, Error::WriteConflict) {
      info!("table {table_name} already set compaction state");
      return Ok(None);
    }
    return Err(err);
  };

  let new_table = tables.create_handle(&table_meta)?.try_pin().unwrap();
  tables.insert(new_table.handle().clone());
  index.initialize(new_table.handle())?;

  tx.commit()?;
  Ok(Some((new_table, tx.current_version(), table_meta)))
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

pub struct CompactionConfig {
  pub batch_size: usize,
}

pub struct Compactor {
  incoming: Arc<SegQueue<CompactTask>>,
  in_progress: Arc<SegQueue<CompactionCycle>>,
  cycle: Arc<AtomicCell<Option<CompactionCycle>>>,
  ticker: Box<dyn BackgroundThread<(), Result>>,
  block_cache: Arc<BlockCache>,
  version_visibility: Arc<VersionVisibility>,
  wal: Arc<WAL>,
  recorder: Arc<PageRecorder>,
  meta_table: TableHandleRef,
}
impl Compactor {
  pub fn new(
    block_cache: Arc<BlockCache>,
    tables: Arc<TableMapper>,
    recorder: Arc<PageRecorder>,
    version_visibility: Arc<VersionVisibility>,
    wal: Arc<WAL>,
    event_bus: Arc<EventBus>,
    config: CompactionConfig,
  ) -> Arc<Self> {
    let incoming = SegQueue::new().to_arc();
    let in_progress = SegQueue::new().to_arc();
    let cycle = AtomicCell::new(None).to_arc();
    let ticker = WorkBuilder::new()
      .name("compaction")
      .stack_size(2 << 20)
      .single()
      .interval(
        COMPACTION_INTERVAL,
        compaction_loop(
          incoming.clone(),
          in_progress.clone(),
          tables.clone(),
          block_cache.clone(),
          version_visibility.clone(),
          wal.clone(),
          recorder.clone(),
          event_bus.clone(),
          cycle.clone(),
          config.batch_size,
        ),
      )
      .to_box();

    let this = Arc::new(Self {
      incoming,
      in_progress,
      cycle,
      ticker,
      block_cache,
      version_visibility,
      recorder,
      wal,
      meta_table: tables.meta_table(),
    });
    event_bus.register(&this);
    this
  }

  pub fn close(&self) -> Result {
    if !self.wal.is_available() {
      self.failover();
      return Ok(());
    }
    self.ticker.close();

    let mut cycle = self.cycle.take();
    if self.in_progress.is_empty() && cycle.is_none() {
      return Ok(());
    }

    warn!(
      "compaction in progress {} count left.",
      self.in_progress.len()
    );

    let old_index = BTreeIndex::new(
      CompactionReadPolicy {
        block_cache: self.block_cache.clone(),
        version_visibility: self.version_visibility.clone(),
      }
      .to_arc(),
    );
    let new_index = BTreeIndex::new(CompactionWritePolicy {
      block_cache: self.block_cache.clone(),
      version_visibility: self.version_visibility.clone(),
      recorder: self.recorder.clone(),
    });

    while let Some(mut cycle) = cycle.take().or_else(|| self.in_progress.pop()) {
      if !check_compaction(
        &self.version_visibility,
        &self.wal,
        &self.block_cache,
        &self.recorder,
        &self.meta_table,
        cycle.metadata.get_name(),
      )? {
        continue;
      }

      let mut snapshotter = match cycle.snapshotter.take() {
        Some(v) => v,
        None => old_index.snapshot(cycle.old.handle())?,
      };

      while let Some(snap) = snapshotter.next_snapshot()? {
        new_index.apply_snapshot(snap, cycle.new.handle())?;
      }

      remove_compaction(
        &self.version_visibility,
        &self.wal,
        &self.block_cache,
        &self.recorder,
        &self.meta_table,
        &cycle.metadata,
      )?;
    }
    Ok(())
  }

  fn failover(&self) {
    self.ticker.close();
    let _ = self.cycle.take();
    while self.in_progress.pop().is_some() {}
  }
}

impl OwnedSubscription<CompactionCommitted> for Compactor {
  fn handle(&self, event: CompactionCommitted) {
    self.incoming.push(CompactTask::Committed(event));
  }
}
impl OwnedSubscription<CompactionTriggered> for Compactor {
  fn handle(&self, event: CompactionTriggered) {
    self.incoming.push(CompactTask::New(event))
  }
}
impl OwnedSubscription<CompactionPublished> for Compactor {
  fn handle(&self, event: CompactionPublished) {
    self
      .in_progress
      .push(CompactionCycle::new(event.old, event.new, event.metadata))
  }
}
impl SharedSubscription<WALFailed> for Compactor {
  fn handle(&self, _: Arc<WALFailed>) {
    error!("compactor stopped since wal failure detected.");
    self.failover();
  }
}
binding_events!(Compactor {
  owned: [
    CompactionCommitted,
    CompactionTriggered,
    CompactionPublished
  ],
  shared: [WALFailed]
});

fn remove_compaction(
  version_visibility: &VersionVisibility,
  wal: &WAL,
  block_cache: &BlockCache,
  recorder: &PageRecorder,
  meta_table: &TableHandleRef,
  table_metadata: &TableMetadata,
) -> Result<Option<(TxId, TxId)>> {
  let mut tx = MiniTx::start(version_visibility, wal, block_cache, recorder)?;
  let index = BTreeIndex::new(&tx);

  let table_name = table_metadata.get_name();
  if !index.contains(table_name.as_bytes(), meta_table)? {
    warn!("table {table_name} already dropped.");
    return Ok(None);
  }

  if let Err(err) = index.insert_if_matched(
    table_name.as_bytes(),
    Some(table_metadata.to_vec()),
    meta_table,
  ) {
    if matches!(err, Error::WriteConflict) {
      warn!("table {table_name} already dropped.");
      return Ok(None);
    }
    return Err(err);
  };

  tx.commit()?;
  Ok(Some((tx.state.get_id(), tx.current_version())))
}

struct CompactionCycle {
  old: PinnedHandle,
  new: PinnedHandle,
  metadata: TableMetadata,
  snapshotter: Option<Snapshotter<Arc<CompactionReadPolicy>>>,
}
impl CompactionCycle {
  const fn new(old: PinnedHandle, new: PinnedHandle, metadata: TableMetadata) -> Self {
    Self {
      old,
      new,
      metadata,
      snapshotter: None,
    }
  }
}

fn check_compaction(
  version_visibility: &VersionVisibility,
  wal: &WAL,
  block_cache: &BlockCache,
  recorder: &PageRecorder,
  meta_table: &TableHandleRef,
  table_name: &TableName,
) -> Result<bool> {
  let tx = MiniTx::start(version_visibility, wal, block_cache, recorder)?;
  BTreeIndex::new(&tx).contains(table_name.as_bytes(), meta_table)
}

fn compaction_loop(
  incoming: Arc<SegQueue<CompactTask>>,
  in_progress: Arc<SegQueue<CompactionCycle>>,
  tables: Arc<TableMapper>,
  block_cache: Arc<BlockCache>,
  version_visibility: Arc<VersionVisibility>,
  wal: Arc<WAL>,
  recorder: Arc<PageRecorder>,
  event_bus: Arc<EventBus>,
  cycle: Arc<AtomicCell<Option<CompactionCycle>>>,
  batch_size: usize,
) -> impl FnMut(Option<()>) -> Result {
  let meta_table = tables.meta_table();
  let mut waiting_publish = LinkedList::new();
  let mut waiting_pin = VecDeque::new();

  let old_index = BTreeIndex::new(
    CompactionReadPolicy {
      block_cache: block_cache.clone(),
      version_visibility: version_visibility.clone(),
    }
    .to_arc(),
  );
  let new_index = BTreeIndex::new(CompactionWritePolicy {
    block_cache: block_cache.clone(),
    version_visibility: version_visibility.clone(),
    recorder: recorder.clone(),
  });

  move |_| {
    let cycle_ref = cycle.as_ptr().borrow_mut_unsafe();
    while let Some(task) = incoming.pop() {
      match task {
        CompactTask::Committed(committed) => waiting_publish.push_back((
          committed.old,
          committed.new,
          committed.metadata,
          committed.commit_version,
        )),
        CompactTask::New(trigger) => {
          let old = trigger.old;
          let table_name = old.get_name();
          let Some((new_table, wait_until, metadata)) = create_compaction(
            &version_visibility,
            &wal,
            &block_cache,
            &recorder,
            &tables,
            &meta_table,
            table_name,
          )?
          else {
            continue;
          };

          info!("table {table_name} compacting wait until another tx close.");
          waiting_publish.push_back((old.clone(), new_table, metadata, wait_until));
        }
      }
    }

    let min_version = version_visibility.min_version();
    for (old, new, metadata, _) in
      waiting_publish.extract_if(|(_, _, _, v)| min_version >= *v)
    {
      waiting_pin.push_back((old, new, metadata));
    }

    for (old, new, metadata) in take(&mut waiting_pin) {
      let Some(old) = old.try_pin() else { continue };
      in_progress.push(CompactionCycle::new(old, new, metadata));
    }

    let Some(current) = cycle_ref.as_mut().or_else(|| {
      in_progress
        .pop()
        .map(|v| cycle.as_ptr().borrow_mut_unsafe().insert(v))
    }) else {
      return Ok(());
    };

    let Some(snapshotter) = &mut current.snapshotter else {
      info!(
        "table {} compaction start to create snapshot.",
        current.metadata.get_name()
      );
      current.snapshotter = Some(old_index.snapshot(current.old.handle())?);
      return Ok(());
    };

    if snapshotter.is_done() {
      let Some((owner, version)) = remove_compaction(
        &version_visibility,
        &wal,
        &block_cache,
        &recorder,
        &meta_table,
        &current.metadata,
      )?
      else {
        return Ok(());
      };
      info!(
        "table {} compacting copied record complete.",
        current.metadata.get_name()
      );
      event_bus.publish(DropTableCommitted::new(
        current.old.handle().clone(),
        owner,
        version,
      ));
      *cycle_ref = None;
      return Ok(());
    }

    if !check_compaction(
      &version_visibility,
      &wal,
      &block_cache,
      &recorder,
      &meta_table,
      current.metadata.get_name(),
    )? {
      warn!("table {} already dropped.", current.metadata.get_name());
      return Ok(());
    }

    for _ in 0..batch_size {
      let Some(snap) = snapshotter.next_snapshot()? else {
        break;
      };
      new_index.apply_snapshot(snap, current.new.handle())?;
    }

    Ok(())
  }
}
