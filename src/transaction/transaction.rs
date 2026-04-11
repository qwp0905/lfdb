use std::{sync::Arc, time::Instant};

use crate::{
  cursor::Cursor,
  metrics::MetricsRegistry,
  table::{TableHandle, TableMetadata},
  transaction::{TxOrchestrator, TxSnapshot, TxState},
  Error, Result,
};

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
  created_tables: Vec<Arc<TableHandle>>,
  dropped_tables: Vec<Arc<TableHandle>>,
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
      created_tables: Default::default(),
      dropped_tables: Default::default(),
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

    let cursor = self.open_cursor(self.orchestrator.get_metadata_table());
    if let Some(bytes) = cursor.get(&name.as_bytes())? {
      let metadata = TableMetadata::from_bytes(&bytes)?;
      if let Some(table) = self.orchestrator.get_table(metadata.get_id()) {
        return Ok(self.open_cursor(table));
      }
    }

    Err(Error::TableNotFound(name.to_string()))
  }

  pub fn open_table(&mut self, name: &str) -> Result<Cursor<'_>> {
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
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
    self.orchestrator.commit_table(table.clone());
    let cursor = Cursor::initialize(
      table.clone(),
      &self.orchestrator,
      &self.state,
      &self.snapshot,
      &self.metrics,
    )?;
    self.created_tables.push(table);

    Ok(cursor)
  }

  pub fn drop_table(&mut self, name: &str) -> Result {
    let cursor = self.open_cursor(self.orchestrator.get_metadata_table());
    if let Some(bytes) = cursor.get(&name.as_bytes())? {
      let metadata = TableMetadata::from_bytes(&bytes)?;
      cursor.remove(&name.as_bytes())?;

      if let Some(table) = self.orchestrator.get_table(metadata.get_id()) {
        self.dropped_tables.push(table);
      }
    }

    Ok(())
  }

  pub fn commit(&mut self) -> Result {
    if !self.state.try_commit() {
      return Err(Error::TransactionClosed);
    }
    if let Err(err) = self.orchestrator.commit_tx(self.state.get_id()) {
      self.state.make_available();
      return Err(err);
    }

    while let Some(table) = self.dropped_tables.pop() {
      self.orchestrator.drop_table(table, self.state.get_id());
    }

    self.state.deactive();
    Ok(())
  }

  pub fn abort(&mut self) -> Result {
    if !self.state.try_abort() {
      return Err(Error::TransactionClosed);
    }
    self.orchestrator.abort_tx(self.state.get_id())?;
    while let Some(table) = self.created_tables.pop() {
      self.orchestrator.drop_table(table, self.state.get_id());
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
