use std::{cell::Cell, collections::LinkedList, sync::Arc, time::Duration};

use crossbeam::{atomic::AtomicCell, queue::SegQueue};

use super::{
  BTreeIndex, BlobStorage, CreatablePolicy, DropTableCommitted, ReadonlyPolicy, RecordId,
  Snapshotter, WritablePolicy, WriteOp,
};
use crate::{
  background::{
    BackgroundThread, EventBus, OwnedSubscription, SharedSubscription, ThreadBuilder,
  },
  binding_events,
  cache::{BlockCache, RefedSlot},
  disk::Pointer,
  error, info,
  serialize::Serializable,
  table::{TableHandleRef, TableMapper, TableMetadata, TableName},
  trace,
  transaction::{PageRecorder, TxSnapshot, TxState, VersionVisibility},
  utils::{ToArc, ToBox},
  wal::{TxId, WALFailed, RESERVED_TX, WAL},
  warn, Error, Result,
};

/**
 * Minimal local transaction used by compaction metadata updates.
 *
 * Compaction needs real transaction boundaries for metadata writes: visibility,
 * WAL commit/abort, page recording, and rollback marking. It does not need the
 * full user-facing transaction orchestrator, and this code runs on the single
 * compaction worker, so `MiniTx` keeps only the local pieces required for those
 * internal writes.
 */
struct MiniTx<'a> {
  state: TxState<'a>,
  snapshot: TxSnapshot<'a>,
  block_cache: &'a BlockCache,
  version_visibility: &'a VersionVisibility,
  recorder: &'a PageRecorder,
  wal: &'a WAL,
  blob: &'a BlobStorage,
  committed: Cell<bool>,
  modified: Cell<bool>,
  record_id: Cell<RecordId>,
}
impl<'a> MiniTx<'a> {
  fn start(
    version_visibility: &'a VersionVisibility,
    wal: &'a WAL,
    block_cache: &'a BlockCache,
    recorder: &'a PageRecorder,
    blob: &'a BlobStorage,
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
      blob,
      committed: Cell::new(false),
      modified: Cell::new(false),
      record_id: Cell::new(0),
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
  fn read_blob(
    &self,
    blob_id: super::BlobId,
    offset: super::BlobOffset,
    len: super::BlobLen,
  ) -> Result<crate::disk::AlignedBuf> {
    let blob = self
      .blob
      .get(blob_id)
      .unwrap_or_else(|| unreachable!("blob id {blob_id} must exists"));
    blob.read_at(offset, len)
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
  fn write_blob(&self, data: Vec<u8>) -> Result<super::BlobAppendGuard<'_>> {
    self.blob.append(data)
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
  fn gen_record_id(&self) -> RecordId {
    let id = self.record_id.get();
    self.record_id.set(id + 1);
    id
  }
}

/**
 * Read policy for copying records from the old table during compaction.
 *
 * Compaction copies blob references, not blob bytes. Any attempt to materialize
 * blob contents through this policy means the compaction path crossed the wrong
 * boundary and should panic.
 */
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
  fn read_blob(
    &self,
    _: super::BlobId,
    _: super::BlobOffset,
    _: super::BlobLen,
  ) -> Result<crate::disk::AlignedBuf> {
    unreachable!()
  }
}

/**
 * Write policy for applying snapshot records into the new table.
 *
 * Snapshot application preserves existing blob references. It must not create
 * new blob payloads, so `write_blob` is unreachable for this policy.
 */
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
  fn read_blob(
    &self,
    _: super::BlobId,
    _: super::BlobOffset,
    _: super::BlobLen,
  ) -> Result<crate::disk::AlignedBuf> {
    unreachable!()
  }
}
impl WritablePolicy for CompactionWritePolicy {
  /**
   * Snapshot-copy writes use the reserved transaction id.
   *
   * Copying records into the compacted segment is an engine-owned background
   * write and is not rolled back. The metadata transitions around that segment are
   * still committed through `MiniTx`; the copied pages themselves are written as
   * non-abortable reserved-transaction records.
   */
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
  fn write_blob(&self, _: Vec<u8>) -> Result<super::BlobAppendGuard<'_>> {
    unreachable!()
  }
}

const COMPACTION_INTERVAL: Duration = Duration::from_secs(1);

/**
 * Start compaction by publishing the new physical table segment in metadata.
 *
 * The metadata entry is the durable compaction state for the logical table. Once
 * this update commits, the logical table is represented by the old segment plus
 * the new compaction segment, and transactions can route through that state.
 */
fn create_compaction(
  version_visibility: &VersionVisibility,
  wal: &WAL,
  block_cache: &BlockCache,
  recorder: &PageRecorder,
  tables: &TableMapper,
  blob: &BlobStorage,
  meta_table: &TableHandleRef,
  table_name: &TableName,
) -> Result<Option<(TableHandleRef, TxId, TableMetadata)>> {
  let mut tx = MiniTx::start(version_visibility, wal, block_cache, recorder, blob)?;
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

  if let Err(err) = index.insert_if_matched(
    table_name.as_bytes(),
    WriteOp::Insert(metadata.to_vec()),
    meta_table,
  ) {
    if matches!(err, Error::WriteConflict) {
      info!("table {table_name} already set compaction state");
      return Ok(None);
    }
    return Err(err);
  };

  let new_table = tables.create_handle(&table_meta)?;
  tables.insert(new_table.clone());
  index.initialize(&new_table)?;

  tx.commit()?;
  Ok(Some((new_table, tx.current_version(), table_meta)))
}

pub struct CompactionCommitted {
  old: TableHandleRef,
  new: TableHandleRef,
  metadata: TableMetadata,
  commit_version: TxId,
}
impl CompactionCommitted {
  pub const fn new(
    old: TableHandleRef,
    new: TableHandleRef,
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

/**
 * Resume a compaction whose metadata publication already happened.
 *
 * Runtime triggers must first publish the old/new table-segment pair and wait
 * for that publication to become globally visible. A `CompactionPublished` event
 * represents a compaction that has already passed that publication boundary, so
 * it can be queued directly for copy progress.
 */
pub struct CompactionPublished {
  old: TableHandleRef,
  new: TableHandleRef,
  metadata: TableMetadata,
}
impl CompactionPublished {
  pub const fn new(
    old: TableHandleRef,
    new: TableHandleRef,
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
  ticker: Box<BackgroundThread<()>>,
  block_cache: Arc<BlockCache>,
  version_visibility: Arc<VersionVisibility>,
  wal: Arc<WAL>,
  recorder: Arc<PageRecorder>,
  blob: Arc<BlobStorage>,
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
    blob: Arc<BlobStorage>,
    config: CompactionConfig,
  ) -> Arc<Self> {
    let incoming = SegQueue::new().to_arc();
    let in_progress = SegQueue::new().to_arc();
    let cycle = AtomicCell::new(None).to_arc();
    let ticker = ThreadBuilder::new()
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
          blob.clone(),
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
      blob,
      meta_table: tables.meta_table(),
    });
    event_bus.register(&this);
    this
  }

  /**
   * Stop compaction immediately after WAL failure.
   *
   * WAL write failure makes the engine unavailable because durability can no
   * longer be guaranteed. Compaction is a write path and records metadata/table
   * changes through WAL, so it must not continue once a `WALFailed` event is
   * observed.
   */
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
        &self.blob,
        &self.meta_table,
        cycle.metadata.get_name(),
      )? {
        continue;
      }

      let (Some(old), Some(new)) = (cycle.old.try_pin(), cycle.new.try_pin()) else {
        continue;
      };

      let mut snapshotter = match cycle.snapshotter.take() {
        Some(v) => v,
        None => old_index.snapshot(old.handle())?,
      };

      while let Some(snap) = snapshotter.next_snapshot()? {
        new_index.apply_snapshot(snap, new.handle())?;
      }

      remove_compaction(
        &self.version_visibility,
        &self.wal,
        &self.block_cache,
        &self.recorder,
        &self.blob,
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

/**
 * Finish compaction by removing the compaction marker from metadata.
 *
 * After this commit, the logical table is represented only by the compacted
 * segment. The returned transaction id/version are used to defer physical
 * removal of the old segment until that metadata transition is visible to all
 * active transactions.
 */
fn remove_compaction(
  version_visibility: &VersionVisibility,
  wal: &WAL,
  block_cache: &BlockCache,
  recorder: &PageRecorder,
  blob: &BlobStorage,
  meta_table: &TableHandleRef,
  table_metadata: &TableMetadata,
) -> Result<Option<(TxId, TxId)>> {
  let mut tx = MiniTx::start(version_visibility, wal, block_cache, recorder, blob)?;
  let index = BTreeIndex::new(&tx);

  let table_name = table_metadata.get_name();
  if !index.contains(table_name.as_bytes(), meta_table)? {
    warn!("table {table_name} already dropped.");
    return Ok(None);
  }

  if let Err(err) = index.insert_if_matched(
    table_name.as_bytes(),
    WriteOp::Insert(table_metadata.to_vec()),
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

/**
 * Shared storage for the currently active compaction cycle.
 *
 * The interval worker is the only thread that mutates this while it is running,
 * but shutdown needs to take over the current work context after the worker is
 * closed. Keeping the active cycle outside the worker closure lets `close`
 * continue draining the same compaction synchronously.
 */
struct CompactionCycle {
  old: TableHandleRef,
  new: TableHandleRef,
  metadata: TableMetadata,
  snapshotter: Option<Snapshotter<Arc<CompactionReadPolicy>>>,
}
impl CompactionCycle {
  const fn new(
    old: TableHandleRef,
    new: TableHandleRef,
    metadata: TableMetadata,
  ) -> Self {
    Self {
      old,
      new,
      metadata,
      snapshotter: None,
    }
  }
}

/**
 * Check whether the logical table still exists before continuing compaction.
 *
 * Compaction can race with table drop. The compactor only needs to know whether
 * the logical table is still present in metadata; if it has been removed, the
 * in-progress compaction can be abandoned.
 */
fn check_compaction(
  version_visibility: &VersionVisibility,
  wal: &WAL,
  block_cache: &BlockCache,
  recorder: &PageRecorder,
  blob: &BlobStorage,
  meta_table: &TableHandleRef,
  table_name: &TableName,
) -> Result<bool> {
  let tx = MiniTx::start(version_visibility, wal, block_cache, recorder, blob)?;
  BTreeIndex::new(&tx).contains(table_name.as_bytes(), meta_table)
}

fn run_tick(
  incoming: &SegQueue<CompactTask>,
  in_progress: &SegQueue<CompactionCycle>,
  tables: &TableMapper,
  block_cache: &BlockCache,
  version_visibility: &VersionVisibility,
  wal: &WAL,
  recorder: &PageRecorder,
  event_bus: &EventBus,
  blob: &BlobStorage,
  cycle: &AtomicCell<Option<CompactionCycle>>,
  batch_size: usize,
  waiting_publish: &mut LinkedList<(TableHandleRef, TableHandleRef, TableMetadata, TxId)>,
  meta_table: &TableHandleRef,
  old_index: &BTreeIndex<Arc<CompactionReadPolicy>>,
  new_index: &BTreeIndex<CompactionWritePolicy>,
) -> Result {
  // SAFETY: single threaded access to cycle.
  let cycle_ref = unsafe { &mut *cycle.as_ptr() };
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
          version_visibility,
          wal,
          block_cache,
          recorder,
          tables,
          blob,
          meta_table,
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
    in_progress.push(CompactionCycle::new(old, new, metadata));
  }

  let Some(current) = cycle_ref.as_mut().or_else(|| {
    in_progress
      .pop()
      .map(|v| unsafe { (*cycle.as_ptr()).insert(v) })
  }) else {
    return Ok(());
  };

  let (Some(old), Some(new)) = (current.old.try_pin(), current.new.try_pin()) else {
    return Ok(*cycle_ref = None);
  };

  let Some(snapshotter) = &mut current.snapshotter else {
    info!(
      "table {} compaction start to create snapshot.",
      current.metadata.get_name()
    );
    current.snapshotter = Some(old_index.snapshot(old.handle())?);
    return Ok(());
  };

  if snapshotter.is_done() {
    let Some((owner, version)) = remove_compaction(
      version_visibility,
      wal,
      block_cache,
      recorder,
      blob,
      meta_table,
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
      old.handle().clone(),
      owner,
      version,
    ));
    drop(old);
    drop(new);
    return Ok(*cycle_ref = None);
  }

  if !check_compaction(
    version_visibility,
    wal,
    block_cache,
    recorder,
    blob,
    meta_table,
    current.metadata.get_name(),
  )? {
    warn!("table {} already dropped.", current.metadata.get_name());
    drop(old);
    drop(new);
    return Ok(*cycle_ref = None);
  }

  for _ in 0..batch_size {
    let Some(snap) = snapshotter.next_snapshot()? else {
      break;
    };
    new_index.apply_snapshot(snap, new.handle())?;
  }

  Ok(())
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
  blob: Arc<BlobStorage>,
  cycle: Arc<AtomicCell<Option<CompactionCycle>>>,
  batch_size: usize,
) -> impl FnMut(Option<()>) {
  let meta_table = tables.meta_table();
  // Wait until the compaction metadata publication is globally visible.
  // `waiting_publish` holds compactions whose metadata update has committed, but
  // may still be invisible to transactions that were already active. Once the
  // commit version is below the global minimum visible version, future access can
  // observe the old/new table-segment pair from metadata.
  let mut waiting_publish = LinkedList::new();

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
    run_tick(
      &incoming,
      &in_progress,
      &tables,
      &block_cache,
      &version_visibility,
      &wal,
      &recorder,
      &event_bus,
      &blob,
      &cycle,
      batch_size,
      &mut waiting_publish,
      &meta_table,
      &old_index,
      &new_index,
    )
    .unwrap()
  }
}
