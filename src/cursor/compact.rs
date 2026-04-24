use std::{cell::Cell, collections::VecDeque, ops::Bound, sync::Arc, time::Duration};

use super::{
  BTreeIndex, CreatablePolicy, GarbageCollector, ReadonlyPolicy, RecordData,
  VersionRecord, WritablePolicy,
};
use crate::{
  cache::{BlockCache, WritableSlot},
  disk::Pointer,
  serialize::Serializable,
  table::{MutationHandle, TableHandle, TableMapper, TableMetadata},
  thread::BackgroundThread,
  transaction::{PageRecorder, TxSnapshot, TxState, VersionVisibility},
  utils::LogFilter,
  wal::{TxId, RESERVED_TX, WAL},
  Result,
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
  gc: &'a GarbageCollector,
  committed: Cell<bool>,
  modified: Cell<bool>,
}
impl<'a> MiniTx<'a> {
  fn start(
    version_visibility: &'a VersionVisibility,
    wal: &'a WAL,
    block_cache: &'a BlockCache,
    recorder: &'a PageRecorder,
    gc: &'a GarbageCollector,
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
      gc,
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
      self.wal.append_abort(self.state.get_id())?;
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
  fn is_visible(&self, owner: TxId, version: TxId) -> bool {
    let current = self.state.get_id();
    if owner == current {
      return true;
    }
    version <= current && self.snapshot.is_visible(&owner)
  }

  fn fetch_slot(
    &self,
    pointer: Pointer,
    table: &Arc<TableHandle>,
  ) -> Result<crate::cache::CacheSlot<'_>> {
    self.block_cache.peek(pointer, table.clone())
  }
}
impl<'a> WritablePolicy for &MiniTx<'a> {
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut WritableSlot<'_>,
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

  fn after_update_entry(&self, entry: Pointer, table: &Arc<TableHandle>) {
    self.gc.mark(table.clone(), entry);
  }
}
impl<'a> CreatablePolicy for &MiniTx<'a> {
  fn is_conflict(&self, owner: TxId) -> bool {
    owner != self.state.get_id() && self.snapshot.is_active(&owner)
  }

  fn create_record(&self, data: RecordData) -> VersionRecord {
    VersionRecord::new(
      self.state.get_id(),
      self.version_visibility.current_version(),
      data,
    )
  }
}

struct CompactionReadPolicy<'a> {
  block_cache: &'a BlockCache,
  version_visibility: &'a VersionVisibility,
}
impl<'a> ReadonlyPolicy for CompactionReadPolicy<'a> {
  fn is_visible(&self, owner: TxId, _: TxId) -> bool {
    !self.version_visibility.is_aborted(&owner)
  }

  fn fetch_slot(
    &self,
    pointer: Pointer,
    table: &Arc<TableHandle>,
  ) -> Result<crate::cache::CacheSlot<'_>> {
    self.block_cache.peek(pointer, table.clone())
  }
}

struct CompactionWritePolicy<'a> {
  block_cache: &'a BlockCache,
  version_visibility: &'a VersionVisibility,
  recorder: &'a PageRecorder,
  gc: &'a GarbageCollector,
}
impl<'a> ReadonlyPolicy for CompactionWritePolicy<'a> {
  fn is_visible(&self, owner: TxId, _: TxId) -> bool {
    !self.version_visibility.is_aborted(&owner)
  }

  fn fetch_slot(
    &self,
    pointer: Pointer,
    table: &Arc<TableHandle>,
  ) -> Result<crate::cache::CacheSlot<'_>> {
    self.block_cache.peek(pointer, table.clone())
  }
}
impl<'a> WritablePolicy for CompactionWritePolicy<'a> {
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut WritableSlot<'_>,
    data: &T,
    table: &Arc<TableHandle>,
  ) -> Result {
    self
      .recorder
      .serialize_and_log(RESERVED_TX, table.metadata().get_id(), slot, data)
  }

  fn after_update_entry(&self, entry: Pointer, table: &Arc<TableHandle>) {
    self.gc.mark(table.clone(), entry)
  }
}

pub const COMPACTION_INTERVAL: Duration = Duration::from_secs(1);

pub fn wait_compaction(
  tables: Arc<TableMapper>,
  block_cache: Arc<BlockCache>,
  versions: Arc<VersionVisibility>,
  wal: Arc<WAL>,
  recorder: Arc<PageRecorder>,
  gc: Arc<GarbageCollector>,
  logger: LogFilter,
  compaction: Arc<dyn BackgroundThread<(MutationHandle, MutationHandle), Result>>,
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
            let mut tx = MiniTx::start(&versions, &wal, &block_cache, &recorder, &gc)?;

            let index = BTreeIndex::new(&tx);

            let mut metadata =
              match index.get(table_name.as_bytes(), &meta_table)?.flatten() {
                Some(bytes) => TableMetadata::from_bytes(&bytes)?,
                None => return Ok(()),
              };

            if old.metadata().get_id() != metadata.get_id()
              || metadata.get_compaction_id().is_some()
            {
              logger.trace(|| {
                format!("table {table_name} compacting skipped since already compacted.")
              });
              return Ok(());
            }

            logger.info(|| format!("table {table_name} compacting triggered."));
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

          logger.info(|| {
            format!("table {table_name} compacting wait until another tx close.")
          });

          triggered.push((old, new_table, wait_until));
        }
      };
    }

    let min_version = versions.min_version();
    for (old, new, _) in triggered.extract_if(.., |(_, _, v)| min_version >= *v) {
      waited.push_back((old, new));
    }

    for _ in 0..waited.len() {
      match waited.pop_front() {
        Some((old, new)) => match old.try_mutation() {
          Some(old) => compaction.dispatch((old, new)),
          None => waited.push_back((old, new)),
        },
        None => return Ok(()),
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
  gc: Arc<GarbageCollector>,
  logger: LogFilter,
  after_compaction: Arc<
    dyn BackgroundThread<(MutationHandle, MutationHandle, TxId, TxId)>,
  >,
) -> impl Fn((MutationHandle, MutationHandle)) -> Result {
  let meta_table = tables.meta_table();
  move |(old, new)| {
    do_compaction(
      &block_cache,
      &versions,
      &wal,
      &recorder,
      &gc,
      &logger,
      &meta_table,
      old,
      new,
      &after_compaction,
    )
  }
}

fn do_compaction(
  block_cache: &BlockCache,
  version_visibility: &VersionVisibility,
  wal: &WAL,
  recorder: &PageRecorder,
  gc: &GarbageCollector,
  logger: &LogFilter,
  meta_table: &Arc<TableHandle>,
  old_table: MutationHandle,
  new: MutationHandle,
  after_compaction: &Arc<
    dyn BackgroundThread<(MutationHandle, MutationHandle, TxId, TxId)>,
  >,
) -> Result {
  let table_name = old_table.metadata().get_name();
  logger.info(|| format!("table {table_name} compacting begin."));
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
      gc,
    });

    'compaction: loop {
      for _ in 0..1000 {
        match old_snapshot.snapshot()? {
          Some(snap) => {
            new_index.apply_snapshot(snap, new.handle())?;
            moved_count += 1;
          }
          None => break 'compaction,
        }
      }

      let tx = MiniTx::start(version_visibility, wal, block_cache, recorder, gc)?;
      if !BTreeIndex::new(&tx).contains(table_name.as_bytes(), meta_table)? {
        logger.warn(|| format!("table {table_name} already dropped."));
        return Ok(());
      }
    }
  }

  logger.info(|| {
    format!(
      "table {table_name} compacting copied {} count record complete.",
      moved_count,
    )
  });

  let (tx_id, version) = {
    let mut tx = MiniTx::start(version_visibility, wal, block_cache, recorder, gc)?;
    let index = BTreeIndex::new(&tx);

    if !index.contains(table_name.as_bytes(), meta_table)? {
      logger.warn(|| format!("table {table_name} already dropped."));
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

  logger.info(|| format!("table {table_name} compacting totally complete."));
  after_compaction.dispatch((old_table, new, tx_id, version));

  Ok(())
}

pub fn after_compaction(
  gc: Arc<GarbageCollector>,
  version_visibility: Arc<VersionVisibility>,
) -> impl FnMut(Option<(MutationHandle, MutationHandle, TxId, TxId)>) {
  let mut buffered = Vec::new();
  move |data| {
    if let Some(v) = data {
      buffered.push(v);
    }

    let min_version = version_visibility.min_version();
    for (old, _, tx_id, version) in
      buffered.extract_if(.., |(_, _, _, v)| min_version >= *v)
    {
      gc.release_table(old.into_inner(), tx_id, version);
    }
  }
}
