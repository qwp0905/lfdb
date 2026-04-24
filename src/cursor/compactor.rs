use std::{cell::Cell, ops::Bound, sync::Arc, time::Duration};

use super::{
  BTreeIndex, CreatablePolicy, GarbageCollector, ReadonlyPolicy, RecordData,
  VersionRecord, WritablePolicy,
};
use crate::{
  buffer_pool::{BufferPool, WritableSlot},
  disk::Pointer,
  serialize::Serializable,
  table::{MutationHandle, TableHandle, TableMapper, TableMetadata},
  transaction::{PageRecorder, TxSnapshot, TxState, VersionVisibility},
  utils::LogFilter,
  wal::{TxId, RESERVED_TX, WAL},
  Error, Result,
};

pub enum CompactTask {
  Resume(Arc<TableHandle>, MutationHandle),
  New(Arc<TableHandle>),
}
impl CompactTask {
  fn name(&self) -> String {
    match self {
      CompactTask::Resume(table, _) => table,
      CompactTask::New(table) => table,
    }
    .metadata()
    .get_name()
    .to_string()
  }
}

struct MiniTx<'a> {
  state: TxState<'a>,
  snapshot: TxSnapshot<'a>,
  buffer_pool: &'a BufferPool,
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
    buffer_pool: &'a BufferPool,
    recorder: &'a PageRecorder,
    gc: &'a GarbageCollector,
  ) -> Result<Self> {
    let (state, snapshot) = version_visibility.new_transaction();
    wal.append_start(state.get_id())?;
    Ok(Self {
      state,
      snapshot,
      buffer_pool,
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
  ) -> Result<crate::buffer_pool::Slot<'_>> {
    self.buffer_pool.peek(pointer, table.clone())
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
  buffer_pool: &'a BufferPool,
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
  ) -> Result<crate::buffer_pool::Slot<'_>> {
    self.buffer_pool.peek(pointer, table.clone())
  }
}

struct CompactionWritePolicy<'a> {
  buffer_pool: &'a BufferPool,
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
  ) -> Result<crate::buffer_pool::Slot<'_>> {
    self.buffer_pool.peek(pointer, table.clone())
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

pub fn handle_compaction(
  tables: Arc<TableMapper>,
  buffer_pool: Arc<BufferPool>,
  versions: Arc<VersionVisibility>,
  wal: Arc<WAL>,
  recorder: Arc<PageRecorder>,
  gc: Arc<GarbageCollector>,
  logger: LogFilter,
) -> impl Fn(CompactTask) -> Result {
  let meta_table = tables.meta_table();
  move |task| {
    let table_name = task.name();
    let (old_table, new) = match task {
      CompactTask::Resume(old, new) => (old, new),
      CompactTask::New(old) => {
        let (new_table, wait_until) = {
          let mut tx = MiniTx::start(&versions, &wal, &buffer_pool, &recorder, &gc)?;

          let index = BTreeIndex::new(&tx);

          let mut metadata =
            match index.get(table_name.as_bytes(), &meta_table)?.flatten() {
              Some(bytes) => TableMetadata::from_bytes(&bytes)?,
              None => return Err(Error::TableNotFound(table_name)),
            };

          if old.metadata().get_id() != metadata.get_id() {
            return Ok(());
          }
          if metadata.get_compaction_id().is_some() {
            return Ok(());
          }

          logger.info(|| format!("table {table_name} compacting triggered."));
          let table_meta = tables.create_metadata(&table_name);
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
          (new_table, tx.state.get_id())
        };

        logger
          .info(|| format!("table {table_name} compacting wait until another tx close."));

        while versions.min_version() < wait_until {
          std::thread::sleep(Duration::from_secs(1));
        }

        (old, new_table)
      }
    };

    let old_table = loop {
      match old_table.try_mutation() {
        Some(handle) => break handle,
        None => std::thread::sleep(Duration::from_secs(1)),
      }
    };

    logger.info(|| format!("table {table_name} compacting begin."));
    let mut moved_count = 0;

    {
      let old_index = BTreeIndex::new(CompactionReadPolicy {
        buffer_pool: &buffer_pool,
        version_visibility: &versions,
      });

      let mut old_snapshot =
        old_index.scan(old_table.handle(), &Bound::Unbounded, &Bound::Unbounded)?;

      let new_index = BTreeIndex::new(CompactionWritePolicy {
        buffer_pool: &buffer_pool,
        version_visibility: &versions,
        recorder: &recorder,
        gc: &gc,
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

        let tx = MiniTx::start(&versions, &wal, &buffer_pool, &recorder, &gc)?;
        if !BTreeIndex::new(&tx).contains(table_name.as_bytes(), &meta_table)? {
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

    let tx_id = {
      let mut tx = MiniTx::start(&versions, &wal, &buffer_pool, &recorder, &gc)?;
      let index = BTreeIndex::new(&tx);

      if !index.contains(table_name.as_bytes(), &meta_table)? {
        logger.warn(|| format!("table {table_name} already dropped."));
        return Ok(());
      }

      index.insert_if_matched(
        table_name.as_bytes(),
        new.metadata().to_vec(),
        &meta_table,
      )?;

      tx.commit()?;
      tx.state.get_id()
    };

    gc.release_table(old_table.handle().clone(), tx_id);
    logger.info(|| format!("table {table_name} compacting totally complete."));

    Ok(())
  }
}
