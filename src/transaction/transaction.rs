use std::{marker::PhantomData, sync::Arc, time::Instant};

use crate::{
  cursor::{Cursor, META_TABLE_HEADER},
  disk::Pointer,
  metrics::MetricsRegistry,
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
  created_tables: Vec<(String, Pointer)>,
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
  fn open_cursor(&self, header: Pointer) -> Cursor<'_> {
    Cursor::new(
      header,
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

    if let Some(header) = self.orchestrator.get_table(name) {
      return Ok(self.open_cursor(header));
    }
    Err(Error::TableNotFound(name.to_string()))
  }

  pub fn open_table(&mut self, name: &str) -> Result<Cursor<'_>> {
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
    }

    if let Some(header) = self.orchestrator.reserve_table(name)? {
      return Ok(self.open_cursor(header));
    }

    let meta = self.open_cursor(META_TABLE_HEADER);
    if let Some(bytes) = meta.get(&name.as_bytes())? {
      if let Ok(bytes) = bytes.try_into() {
        let header = Pointer::from_le_bytes(bytes);
        self.orchestrator.create_table(name.to_string(), header);
        return Ok(self.open_cursor(header));
      }
    };

    let cursor = Cursor::initialize(
      &self.orchestrator,
      &self.state,
      &self.snapshot,
      &self.metrics,
    )?;
    meta.insert(
      name.as_bytes().to_vec(),
      cursor.get_header().to_le_bytes().to_vec(),
    )?;
    self
      .created_tables
      .push((name.to_string(), cursor.get_header()));
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

    while let Some((name, header)) = self.created_tables.pop() {
      self.orchestrator.create_table(name, header);
    }
    self.state.deactive();
    Ok(())
  }

  pub fn abort(&mut self) -> Result {
    if !self.state.try_abort() {
      return Err(Error::TransactionClosed);
    }
    self.orchestrator.abort_tx(self.state.get_id())?;

    while let Some((name, header)) = self.created_tables.pop() {
      self.orchestrator.drop_table(&name, header);
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
