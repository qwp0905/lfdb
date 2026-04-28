use std::{sync::Arc, time::Instant};

use super::{TxContext, TxOrchestrator, TxSnapshot, TxState};
use crate::{
  cursor::Cursor,
  metrics::MetricsRegistry,
  table::{MutationHandle, TableHandle, TableMetadata, MAX_TABLE_NAME_LEN},
  Error, Result,
};

/**
 * A handle for a single transaction, providing table operations.
 * Automatically aborts on drop if not committed.
 */
pub struct Transaction<'a> {
  orchestrator: &'a TxOrchestrator,
  context: TxContext<'a>,
  metrics: &'a MetricsRegistry,
  tx_start: Option<Instant>,
  created_tables: Vec<Arc<TableHandle>>,
  dropped_tables: Vec<Arc<TableHandle>>,
  compacted_tables: Vec<(Arc<TableHandle>, MutationHandle)>,
}
impl<'a> Transaction<'a> {
  pub fn new(
    orchestrator: &'a TxOrchestrator,
    state: TxState<'a>,
    snapshot: TxSnapshot<'a>,
    metrics: &'a MetricsRegistry,
  ) -> Self {
    let tx_start = metrics.transaction_start.start();
    let context = TxContext::new(orchestrator, state, snapshot);
    Self {
      orchestrator,
      context,
      metrics,
      tx_start,
      created_tables: Vec::new(),
      dropped_tables: Vec::new(),
      compacted_tables: Vec::new(),
    }
  }

  #[inline]
  fn open_cursor(
    &self,
    table: Arc<TableHandle>,
    compaction: Option<Arc<TableHandle>>,
  ) -> Cursor<'_> {
    Cursor::new(table, compaction, &self.context, &self.metrics)
  }

  pub fn table(&self, name: &str) -> Result<Cursor<'_>> {
    if name.len() > MAX_TABLE_NAME_LEN {
      return Err(Error::TableNameExceeded(MAX_TABLE_NAME_LEN, name.len()));
    }

    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }

    let cursor = self.open_cursor(self.orchestrator.get_metadata_table(), None);
    if let Some(bytes) = cursor.get(&name.as_bytes())? {
      let metadata = TableMetadata::from_bytes(&bytes)?;
      if let Some(table) = self.orchestrator.get_table(metadata.get_id()) {
        return Ok(
          self.open_cursor(
            table,
            metadata
              .get_compaction_id()
              .and_then(|id| self.orchestrator.get_table(id)),
          ),
        );
      }
    }

    Err(Error::TableNotFound(name.to_string()))
  }

  pub fn open_table(&mut self, name: &str) -> Result<Cursor<'_>> {
    if name.len() > MAX_TABLE_NAME_LEN {
      return Err(Error::TableNameExceeded(MAX_TABLE_NAME_LEN, name.len()));
    }

    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }

    let meta_cursor = self.open_cursor(self.orchestrator.get_metadata_table(), None);
    if let Some(bytes) = meta_cursor.get(&name.as_bytes())? {
      let metadata = TableMetadata::from_bytes(&bytes)?;
      if let Some(table) = self.orchestrator.get_table(metadata.get_id()) {
        return Ok(
          self.open_cursor(
            table,
            metadata
              .get_compaction_id()
              .and_then(|id| self.orchestrator.get_table(id)),
          ),
        );
      }
    }

    let table_meta = self.orchestrator.create_table_metadata(name);
    meta_cursor.insert(name.as_bytes().to_vec(), table_meta.to_vec())?;

    let table = self.orchestrator.open_table(&table_meta)?;
    let cursor = Cursor::initialize(table.clone(), &self.context, &self.metrics)?;
    self.orchestrator.commit_table(table.clone());
    self.created_tables.push(table);

    Ok(cursor)
  }

  pub fn drop_table(&mut self, name: &str) -> Result {
    if name.len() > MAX_TABLE_NAME_LEN {
      return Err(Error::TableNameExceeded(MAX_TABLE_NAME_LEN, name.len()));
    }

    let cursor = self.open_cursor(self.orchestrator.get_metadata_table(), None);
    let metadata = match cursor.get(&name.as_bytes())? {
      Some(bytes) => TableMetadata::from_bytes(&bytes)?,
      None => return Ok(()),
    };

    cursor.remove(&name.as_bytes())?;

    if let Some(table) = self.orchestrator.get_table(metadata.get_id()) {
      self.dropped_tables.push(table);
    }

    if let Some(table) = metadata
      .get_compaction_id()
      .and_then(|id| self.orchestrator.get_table(id))
    {
      self.dropped_tables.push(table);
    }

    Ok(())
  }

  /**
   * Trigger table compaction.
   * It runs in the background and may result in reduced read performance, but it does not block anything.
   */
  pub fn compact_table(&mut self, name: &str) -> Result {
    let cursor = self.open_cursor(self.orchestrator.get_metadata_table(), None);

    let mut metadata = match cursor.get(&name.as_bytes())? {
      Some(bytes) => TableMetadata::from_bytes(&bytes)?,
      None => return Err(Error::TableNotFound(name.to_string())),
    };

    if metadata.get_compaction_id().is_some() {
      return Ok(());
    }

    let old = match self.orchestrator.get_table(metadata.get_id()) {
      Some(table) => table,
      None => return Err(Error::TableNotFound(name.to_string())),
    };

    let table_meta = self.orchestrator.create_table_metadata(name);
    metadata.set_compaction(&table_meta);

    cursor.insert(name.as_bytes().to_vec(), metadata.to_vec())?;

    let new_table = self
      .orchestrator
      .open_table(&table_meta)?
      .try_mutation()
      .unwrap();

    Cursor::initialize(new_table.handle().clone(), &self.context, &self.metrics)?;
    self.orchestrator.commit_table(new_table.handle().clone());
    self.compacted_tables.push((old, new_table));

    Ok(())
  }

  pub fn commit(&mut self) -> Result {
    let state = self.context.state();
    if !state.try_commit() {
      return Err(Error::TransactionClosed);
    }
    if !self.context.is_modified() {
      state.deactive();
      return Ok(());
    }

    let id = state.get_id();
    if let Err(err) = self.orchestrator.commit_tx(id) {
      state.make_available();
      return Err(err);
    }

    state.deactive();
    let version = self.orchestrator.current_version();

    for _ in self.created_tables.drain(..) {}
    for table in self.dropped_tables.drain(..) {
      self.orchestrator.drop_table(table, id, version);
    }
    for (old, new) in self.compacted_tables.drain(..) {
      self.orchestrator.compact_table(old, new, version);
    }

    Ok(())
  }

  pub fn abort(&mut self) -> Result {
    let state = self.context.state();
    if !state.try_abort() {
      return Err(Error::TransactionClosed);
    }
    if !self.context.is_modified() {
      state.deactive();
      return Ok(());
    }

    let id = state.get_id();
    self.orchestrator.abort_tx(id)?;
    state.deactive();

    self.clear();
    Ok(())
  }

  fn clear(&mut self) {
    let id = self.context.state().get_id();
    let version = self.orchestrator.current_version();

    for table in self.created_tables.drain(..) {
      self.orchestrator.drop_table(table, id, version);
    }
    for (_, new) in self.compacted_tables.drain(..) {
      self.orchestrator.drop_table(new.into_inner(), id, version);
    }
  }
}
impl<'a> Drop for Transaction<'a> {
  fn drop(&mut self) {
    let _ = self.abort();
    self.metrics.transaction_start.record(self.tx_start.take());
    self.clear();
  }
}
