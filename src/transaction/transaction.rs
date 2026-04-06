use std::{marker::PhantomData, sync::Arc, time::Instant};

use crate::{
  cursor::Cursor,
  metrics::MetricsRegistry,
  table::{ReserveResult, TableHandle, TableMetadata},
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
  created_tables: Vec<(String, Arc<TableHandle>)>,
  _marker: PhantomData<*const ()>,
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
      created_tables: Vec::new(),
      _marker: Default::default(),
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

    if let Some(table) = self.orchestrator.get_table(name) {
      return Ok(self.open_cursor(table));
    }
    Err(Error::TableNotFound(name.to_string()))
  }

  pub fn open_table(&mut self, name: &str) -> Result<Cursor<'_>> {
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
    }

    match self.orchestrator.reserve_table(name)? {
      ReserveResult::Found(table) => return Ok(self.open_cursor(table)),
      ReserveResult::Reserved => return Err(Error::WriteConflict),
      ReserveResult::New => {}
    };

    let meta = self.open_cursor(self.orchestrator.get_metadata_table());
    if let Some(bytes) = meta.get(&name.as_bytes())? {
      if let Ok(table_meta) = TableMetadata::from_bytes(&bytes) {
        let table = self.orchestrator.create_table(table_meta)?;
        self
          .orchestrator
          .open_table(name.to_string(), table.clone());
        return Ok(self.open_cursor(table));
      }
    }

    let table_meta = self.orchestrator.create_table_metadata(name);
    meta.insert(name.as_bytes().to_vec(), table_meta.to_vec())?;

    let table = self.orchestrator.create_table(table_meta)?;
    let cursor = Cursor::initialize(
      table.clone(),
      &self.orchestrator,
      &self.state,
      &self.snapshot,
      &self.metrics,
    )?;
    self.created_tables.push((name.to_string(), table));

    Ok(cursor)
  }

  pub fn commit(&mut self) -> Result {
    if !self.state.try_commit() {
      return Err(Error::TransactionClosed);
    }
    if let Err(err) = self.orchestrator.commit_tx(self.state.get_id()) {
      self.state.make_available();
      return Err(err);
    }

    while let Some((name, table)) = self.created_tables.pop() {
      self.orchestrator.open_table(name, table);
    }
    self.state.deactive();
    Ok(())
  }

  pub fn abort(&mut self) -> Result {
    if !self.state.try_abort() {
      return Err(Error::TransactionClosed);
    }
    self.orchestrator.abort_tx(self.state.get_id())?;

    while let Some((name, table)) = self.created_tables.pop() {
      self.orchestrator.drop_table(&name, table)?;
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
