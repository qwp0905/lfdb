use std::{collections::HashMap, sync::Arc, time::Instant};

use crate::{
  cursor::Cursor,
  metrics::MetricsRegistry,
  table::{DropResult, PinnedHandle, ReserveResult, TableHandle},
  transaction::{TxOrchestrator, TxSnapshot, TxState},
  utils::SpinRwLock,
  Error, Result,
};

enum TableEntry {
  Created(Arc<TableHandle>),
  Dropped(Arc<TableHandle>),
  DroppedInMemory(Arc<TableHandle>),
}

/**
 * A handle for a single transaction, providing table operations.
 * Must be used on a single thread; cross-thread behavior is untested.
 * Automatically aborts on drop if not committed.
 */
pub struct Transaction<'a> {
  orchestrator: Arc<TxOrchestrator>,
  state: TxState<'a>,
  snapshot: TxSnapshot<'a>,
  metrics: Arc<MetricsRegistry>,
  tx_start: Option<Instant>,
  modified_tables: SpinRwLock<HashMap<String, TableEntry>>,
}
impl<'a> Transaction<'a> {
  pub fn new(
    orchestrator: Arc<TxOrchestrator>,
    state: TxState<'a>,
    snapshot: TxSnapshot<'a>,
    metrics: Arc<MetricsRegistry>,
  ) -> Self {
    let tx_start = metrics.transaction_start.start();
    Self {
      orchestrator,
      state,
      snapshot,
      metrics,
      tx_start,
      modified_tables: Default::default(),
    }
  }

  #[inline]
  fn open_cursor(&self, table: PinnedHandle) -> Cursor<'_> {
    Cursor::new(
      table,
      &self.orchestrator,
      &self.state,
      &self.snapshot,
      &self.metrics,
    )
  }

  pub fn table(&self, name: &str) -> Result<Cursor<'_>> {
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
    }

    if let Some(entry) = self.modified_tables.read().get(name) {
      match entry {
        TableEntry::Created(table) => {
          return Ok(self.open_cursor(PinnedHandle::new(table.clone())))
        }
        TableEntry::Dropped(_) | TableEntry::DroppedInMemory(_) => {
          return Err(Error::TableNotFound(name.to_string()))
        }
      }
    }

    if let Some(table) = self.orchestrator.get_table(name) {
      return Ok(self.open_cursor(table));
    }
    Err(Error::TableNotFound(name.to_string()))
  }

  pub fn open_table(&self, name: &str) -> Result<Cursor<'_>> {
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
    }

    if let Some(entry) = self.modified_tables.read().get(name) {
      match entry {
        TableEntry::Created(table) => {
          return Ok(self.open_cursor(PinnedHandle::new(table.clone())))
        }
        TableEntry::Dropped(_) | TableEntry::DroppedInMemory(_) => {
          return Err(Error::TableAlreadyDropped(name.to_string()))
        }
      }
    }

    match self.orchestrator.reserve_table(name)? {
      ReserveResult::Found(table) => return Ok(self.open_cursor(table)),
      ReserveResult::Reserved => return Err(Error::WriteConflict),
      ReserveResult::New => {}
    };

    let table_meta = self.orchestrator.create_table_metadata();
    let meta_cursor = self.open_cursor(self.orchestrator.get_metadata_table());
    meta_cursor.insert(name.as_bytes().to_vec(), table_meta.to_vec())?;

    let table = self.orchestrator.create_table(table_meta)?;
    let cursor = Cursor::initialize(
      PinnedHandle::new(table.clone()),
      &self.orchestrator,
      &self.state,
      &self.snapshot,
      &self.metrics,
    )?;
    self
      .modified_tables
      .write()
      .insert(name.to_string(), TableEntry::Created(table));

    Ok(cursor)
  }

  pub fn drop_table(&self, name: &str) -> Result {
    {
      let mut tables = self.modified_tables.write();
      match (tables.get(name), self.orchestrator.try_drop_table(name)) {
        (Some(TableEntry::Created(table)), _) => {
          let cloned = table.clone();
          tables.insert(name.to_string(), TableEntry::DroppedInMemory(cloned));
        }
        (Some(TableEntry::Dropped(_)), _)
        | (Some(TableEntry::DroppedInMemory(_)), _)
        | (None, DropResult::NotFound) => return Ok(()),
        (None, DropResult::Reserved(table)) => {
          tables.insert(name.to_string(), TableEntry::Dropped(table));
        }
        (None, DropResult::Conflict) => return Err(Error::WriteConflict),
      }
    }

    let cursor = self.open_cursor(self.orchestrator.get_metadata_table());
    cursor.remove(&name.as_bytes())?;

    Ok(())
  }

  pub fn commit(&self) -> Result {
    if !self.state.try_commit() {
      return Err(Error::TransactionClosed);
    }
    if let Err(err) = self.orchestrator.commit_tx(self.state.get_id()) {
      self.state.make_available();
      return Err(err);
    }

    for (name, entry) in self.modified_tables.write().drain() {
      match entry {
        TableEntry::Created(table) => self.orchestrator.open_table(name, table),
        TableEntry::Dropped(table) => self.orchestrator.drop_table(&name, table),
        TableEntry::DroppedInMemory(table) => self.orchestrator.drop_table(&name, table),
      }
    }

    self.state.deactive();
    Ok(())
  }

  pub fn abort(&self) -> Result {
    if !self.state.try_abort() {
      return Err(Error::TransactionClosed);
    }
    self.orchestrator.abort_tx(self.state.get_id())?;

    for (name, entry) in self.modified_tables.write().drain() {
      match entry {
        TableEntry::Created(table) => self.orchestrator.drop_table(&name, table),
        TableEntry::Dropped(table) => self.orchestrator.open_table(name, table),
        TableEntry::DroppedInMemory(table) => self.orchestrator.drop_table(&name, table),
      };
    }
    self.state.deactive();
    Ok(())
  }
}
impl<'a> Drop for Transaction<'a> {
  fn drop(&mut self) {
    let _ = self.abort();
    self.metrics.transaction_start.record(self.tx_start.take());
  }
}
