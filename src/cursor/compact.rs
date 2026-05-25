use std::{
  cell::Cell, collections::VecDeque, mem::take, ops::Bound, sync::Arc, time::Duration,
};

use crossbeam::queue::SegQueue;

use super::{BTreeIndex, CreatablePolicy, GCMarking, ReadonlyPolicy, WritablePolicy};
use crate::{
  cache::{BlockCache, RefedSlot},
  disk::Pointer,
  info,
  serialize::Serializable,
  table::{MutationHandle, TableHandle, TableMapper, TableMetadata},
  thread::{BackgroundThread, WorkBuilder},
  trace,
  transaction::{PageRecorder, TxSnapshot, TxState, VersionVisibility},
  utils::{DoubleBuffer, ToArc, ToBox},
  wal::{TxId, RESERVED_TX, WAL},
  warn, Result,
};

pub enum CompactTask {
  New(Arc<TableHandle>),
  Wait(Arc<TableHandle>, MutationHandle, TxId),
}

struct MiniTx<'a> {
  state: TxState<'a>,
  snapshot: TxSnapshot<'a>,
  block_cache: &'a BlockCache,
  version_visibility: &'a VersionVisibility,
  recorder: &'a PageRecorder,
  wal: &'a WAL,
  committed: Cell<bool>,
  modified: Cell<bool>,
  modified_entries: SegQueue<(Arc<TableHandle>, Pointer)>,
  gc_queue: &'a DoubleBuffer<GCMarking>,
}
impl<'a> MiniTx<'a> {
  fn start(
    version_visibility: &'a VersionVisibility,
    wal: &'a WAL,
    block_cache: &'a BlockCache,
    recorder: &'a PageRecorder,
    gc_queue: &'a DoubleBuffer<GCMarking>,
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
      modified_entries: SegQueue::new(),
      gc_queue,
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
    let version = self.version_visibility.current_version();
    while let Some((table, ptr)) = self.modified_entries.pop() {
      self
        .gc_queue
        .push(GCMarking::new(table, ptr, self.state.get_id(), version))
    }
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
    let version = self.version_visibility.current_version();
    while let Some((table, ptr)) = self.modified_entries.pop() {
      self
        .gc_queue
        .push(GCMarking::new(table, ptr, self.state.get_id(), version))
    }
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
    table: &Arc<TableHandle>,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.block_cache.read(pointer, table.clone())
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
    table: &Arc<TableHandle>,
  ) -> Result {
    self.recorder.serialize_and_log(
      self.state.get_id(),
      table.metadata().get_id(),
      slot,
      data,
    )?;
    self.modified.set(true);
    Ok(())
  }

  fn alloc_slot(
    &self,
    pointer: Pointer,
    table: &Arc<TableHandle>,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.block_cache.alloc(pointer, table.clone())
  }

  fn when_update_entry(&self, entry_pointer: Pointer, table: &Arc<TableHandle>) {
    self.modified_entries.push((table.clone(), entry_pointer));
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
    table: &Arc<TableHandle>,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.block_cache.read(pointer, table.clone())
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
    table: &Arc<TableHandle>,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.block_cache.read(pointer, table.clone())
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
    table: &Arc<TableHandle>,
  ) -> Result {
    self
      .recorder
      .serialize_and_log(RESERVED_TX, table.metadata().get_id(), slot, data)
  }

  fn alloc_slot(
    &self,
    pointer: Pointer,
    table: &Arc<TableHandle>,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.block_cache.alloc(pointer, table.clone())
  }

  fn when_update_entry(&self, _entry_pointer: Pointer, _table: &Arc<TableHandle>) {}
}

pub const COMPACTION_INTERVAL: Duration = Duration::from_secs(1);

pub fn wait_compaction(
  tables: Arc<TableMapper>,
  block_cache: Arc<BlockCache>,
  versions: Arc<VersionVisibility>,
  wal: Arc<WAL>,
  recorder: Arc<PageRecorder>,
  compaction: Arc<dyn BackgroundThread<(MutationHandle, MutationHandle), Result>>,
  gc_queue: Arc<DoubleBuffer<GCMarking>>,
) -> impl FnMut(Option<CompactTask>) -> Result {
  let meta_table = tables.meta_table();
  let mut triggered = Vec::new();
  let mut waited = VecDeque::new();

  move |task| {
    if let Some(task) = task {
      match task {
        CompactTask::Wait(old, new, until) => triggered.push((old, new, until)),
        CompactTask::New(old) => {
          let table_name = old.metadata().get_name();
          let (new_table, wait_until) = {
            let mut tx =
              MiniTx::start(&versions, &wal, &block_cache, &recorder, &gc_queue)?;

            let index = BTreeIndex::new(&tx);

            let mut metadata =
              match index.get(table_name.as_bytes(), &meta_table)?.flatten() {
                Some(bytes) => TableMetadata::from_bytes(&bytes)?,
                None => return Ok(()),
              };

            if old.metadata().get_id() != metadata.get_id()
              || metadata.get_compaction_id().is_some()
            {
              trace!("table {table_name} compacting skipped since already compacted.");
              return Ok(());
            }

            info!("table {table_name} compacting triggered.");
            let table_meta = tables.create_metadata(table_name);
            metadata.set_compaction(&table_meta);

            index.insert_if_matched(
              table_name.as_bytes(),
              metadata.to_vec(),
              &meta_table,
            )?;

            let new_table = tables.create_handle(&table_meta)?.try_mutation().unwrap();

            tables.insert(new_table.handle().clone());

            index.initialize(new_table.handle())?;

            tx.commit()?;
            (new_table, versions.current_version())
          };

          info!("table {table_name} compacting wait until another tx close.");
          triggered.push((old, new_table, wait_until));
        }
      };
    }

    let min_version = versions.min_version();
    for (old, new, _) in triggered.extract_if(.., |(_, _, v)| min_version >= *v) {
      waited.push_back((old, new));
    }

    for (old, new) in take(&mut waited) {
      match old.try_mutation() {
        Some(old) => compaction.dispatch((old, new)),
        None => waited.push_back((old, new)),
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
  after_compaction: Arc<
    dyn BackgroundThread<(MutationHandle, MutationHandle, TxId, TxId)>,
  >,
  gc_queue: Arc<DoubleBuffer<GCMarking>>,
) -> impl Fn((MutationHandle, MutationHandle)) -> Result {
  let meta_table = tables.meta_table();
  move |(old, new)| {
    do_compaction(
      &block_cache,
      &versions,
      &wal,
      &recorder,
      &meta_table,
      old,
      new,
      &after_compaction,
      &gc_queue,
    )
  }
}

fn do_compaction(
  block_cache: &BlockCache,
  version_visibility: &VersionVisibility,
  wal: &WAL,
  recorder: &PageRecorder,
  meta_table: &Arc<TableHandle>,
  old_table: MutationHandle,
  new: MutationHandle,
  after_compaction: &Arc<
    dyn BackgroundThread<(MutationHandle, MutationHandle, TxId, TxId)>,
  >,
  gc_queue: &DoubleBuffer<GCMarking>,
) -> Result {
  let table_name = old_table.metadata().get_name();
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
      for _ in 0..1000 {
        match old_snapshot.next_snapshot()? {
          Some(snap) => {
            new_index.apply_snapshot(snap, new.handle())?;
            moved_count += 1;
          }
          None => break 'compaction,
        }
      }

      let tx = MiniTx::start(version_visibility, wal, block_cache, recorder, gc_queue)?;
      if !BTreeIndex::new(&tx).contains(table_name.as_bytes(), meta_table)? {
        warn!("table {table_name} already dropped.");
        return Ok(());
      }
    }
  }

  info!("table {table_name} compacting copied {moved_count} count record complete.");

  let (tx_id, version) = {
    let mut tx = MiniTx::start(version_visibility, wal, block_cache, recorder, gc_queue)?;
    let index = BTreeIndex::new(&tx);

    if !index.contains(table_name.as_bytes(), meta_table)? {
      warn!("table {table_name} already dropped.");
      return Ok(());
    }

    index.insert_if_matched(
      table_name.as_bytes(),
      new.metadata().to_vec(),
      &meta_table,
    )?;

    tx.commit()?;
    (tx.state.get_id(), version_visibility.current_version())
  };

  info!("table {table_name} compacting totally complete.");
  after_compaction.dispatch((old_table, new, tx_id, version));

  Ok(())
}

pub const fn after_compaction(
  release_table: Arc<dyn BackgroundThread<(Arc<TableHandle>, TxId, TxId)>>,
  version_visibility: Arc<VersionVisibility>,
) -> impl FnMut(Option<(MutationHandle, MutationHandle, TxId, TxId)>) {
  let mut buffered = Vec::new();
  move |data| {
    if let Some(v) = data {
      buffered.push(v);
    }

    let min_version = version_visibility.min_version();
    for (old, _new, tx_id, version) in
      buffered.extract_if(.., |(_, _, _, v)| min_version >= *v)
    {
      release_table.dispatch((old.into_inner(), tx_id, version));
    }
  }
}

pub struct Compactor {
  wait_compaction: Box<dyn BackgroundThread<CompactTask, Result>>,
  do_compaction: Arc<dyn BackgroundThread<(MutationHandle, MutationHandle), Result>>,
  after_compaction:
    Arc<dyn BackgroundThread<(MutationHandle, MutationHandle, TxId, TxId)>>,
}
impl Compactor {
  pub fn new(
    block_cache: Arc<BlockCache>,
    tables: Arc<TableMapper>,
    recorder: Arc<PageRecorder>,
    version_visibility: Arc<VersionVisibility>,
    wal: Arc<WAL>,
    release_table: Arc<dyn BackgroundThread<(Arc<TableHandle>, TxId, TxId)>>,
    gc_queue: Arc<DoubleBuffer<GCMarking>>,
  ) -> Self {
    let after_compaction = WorkBuilder::new()
      .name("tree after compaction")
      .single()
      .interval(
        COMPACTION_INTERVAL,
        after_compaction(release_table, version_visibility.clone()),
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
        after_compaction.clone(),
        gc_queue.clone(),
      ))
      .to_arc();

    let wait_compaction = WorkBuilder::new()
      .name("tree waiting compaction")
      .single()
      .interval(
        COMPACTION_INTERVAL,
        wait_compaction(
          tables.clone(),
          block_cache.clone(),
          version_visibility.clone(),
          wal.clone(),
          recorder.clone(),
          do_compaction.clone(),
          gc_queue.clone(),
        ),
      )
      .to_box();

    Self {
      wait_compaction,
      do_compaction,
      after_compaction,
    }
  }

  pub fn resume(&self, old: MutationHandle, new: MutationHandle) {
    self.do_compaction.dispatch((old, new));
  }
  pub fn register_wait(&self, old: Arc<TableHandle>, new: MutationHandle, version: TxId) {
    self
      .wait_compaction
      .dispatch(CompactTask::Wait(old, new, version));
  }
  pub fn register_new(&self, table: Arc<TableHandle>) {
    self.wait_compaction.dispatch(CompactTask::New(table));
  }

  pub fn close(&self) {
    self.wait_compaction.close();
    self.do_compaction.close();
    self.after_compaction.close();
  }
}
