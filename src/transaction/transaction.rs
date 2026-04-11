use std::{collections::HashMap, sync::Arc, time::Instant};

use crate::{
  cursor::Cursor,
  metrics::MetricsRegistry,
  table::{TableHandle, TableMetadata},
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
  fn open_cursor(&self, table: Arc<TableHandle>) -> Cursor<'_> {
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
        TableEntry::Created(table) => return Ok(self.open_cursor(table.clone())),
        TableEntry::Dropped(_) | TableEntry::DroppedInMemory(_) => {
          return Err(Error::TableNotFound(name.to_string()))
        }
      }
    }

    let cursor = self.open_cursor(self.orchestrator.get_metadata_table());
    if let Some(bytes) = cursor.get(&name.as_bytes())? {
      let metadata = TableMetadata::from_bytes(&bytes)?;
      if let Some(table) = self.orchestrator.get_table(metadata.get_id()) {
        return Ok(self.open_cursor(table));
      }
    }

    Err(Error::TableNotFound(name.to_string()))
  }

  pub fn open_table(&self, name: &str) -> Result<Cursor<'_>> {
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
    }

    if let Some(entry) = self.modified_tables.read().get(name) {
      match entry {
        TableEntry::Created(table) => return Ok(self.open_cursor(table.clone())),
        TableEntry::Dropped(_) | TableEntry::DroppedInMemory(_) => {
          return Err(Error::TableAlreadyDropped(name.to_string()))
        }
      }
    }

    let meta_cursor = self.open_cursor(self.orchestrator.get_metadata_table());
    if let Some(bytes) = meta_cursor.get(&name.as_bytes())? {
      let metadata = TableMetadata::from_bytes(&bytes)?;
      if let Some(table) = self.orchestrator.get_table(metadata.get_id()) {
        return Ok(self.open_cursor(table));
      }
    }

    let table_meta = self.orchestrator.create_table_metadata(name);
    meta_cursor.insert(name.as_bytes().to_vec(), table_meta.to_vec())?;

    let table = self.orchestrator.open_table(table_meta)?;
    let cursor = Cursor::initialize(
      table.clone(),
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
      if let Some(entry) = tables.get_mut(name) {
        match entry {
          TableEntry::Created(table) => {
            *entry = TableEntry::DroppedInMemory(table.clone());
            return Ok(());
          }
          TableEntry::Dropped(_) | TableEntry::DroppedInMemory(_) => return Ok(()),
        }
      }
    }

    let cursor = self.open_cursor(self.orchestrator.get_metadata_table());
    if let Some(bytes) = cursor.get(&name.as_bytes())? {
      let metadata = TableMetadata::from_bytes(&bytes)?;
      if let Some(table) = self.orchestrator.get_table(metadata.get_id()) {
        self
          .modified_tables
          .write()
          .insert(name.to_string(), TableEntry::Dropped(table.clone()));
      }
    }
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

    for (_, entry) in self.modified_tables.write().drain() {
      match entry {
        TableEntry::Created(table) => self.orchestrator.commit_table(table),
        TableEntry::Dropped(table) => {
          self.orchestrator.drop_table(table, self.state.get_id())
        }
        TableEntry::DroppedInMemory(table) => {
          self.orchestrator.drop_table(table, self.state.get_id())
        }
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

    for (_, entry) in self.modified_tables.write().drain() {
      match entry {
        TableEntry::Created(table) => {
          self.orchestrator.drop_table(table, self.state.get_id())
        }
        TableEntry::Dropped(table) => self.orchestrator.commit_table(table),
        TableEntry::DroppedInMemory(table) => {
          self.orchestrator.drop_table(table, self.state.get_id())
        }
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
