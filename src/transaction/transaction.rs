use std::time::Instant;

use super::{TxContext, TxOrchestrator, TxSnapshot, TxState};
use crate::{
  background::EventBus,
  cursor::{CompactionCommitted, Cursor, DropTableCommitted},
  metrics::MetricsRegistry,
  table::{PinnedHandle, TableHandleRef, TableMetadata, TableName},
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
  created_tables: Vec<TableHandleRef>,
  dropped_tables: Vec<TableHandleRef>,
  compacted_tables: Vec<(TableHandleRef, PinnedHandle, TableMetadata)>,
}
impl<'a> Transaction<'a> {
  pub fn new(
    orchestrator: &'a TxOrchestrator,
    state: TxState<'a>,
    snapshot: TxSnapshot<'a>,
    event_bus: &'a EventBus,
    metrics: &'a MetricsRegistry,
  ) -> Self {
    let tx_start = metrics.transaction_start.start();
    let context = TxContext::new(orchestrator, state, snapshot, event_bus);
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
  const fn open_cursor(
    &self,
    table: TableHandleRef,
    compaction: Option<TableHandleRef>,
  ) -> Cursor<'_> {
    Cursor::new(table, compaction, &self.context, self.metrics)
  }

  pub fn table(&self, name: &str) -> Result<Cursor<'_>> {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }
    let name = TableName::from_str(name)?;

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

      unreachable!("get table must have opened table handle.")
    }

    Err(Error::TableNotFound(name.to_string()))
  }

  pub fn open_table(&mut self, name: &str) -> Result<Cursor<'_>> {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }
    let name = TableName::from_str(name)?;

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

      unreachable!("get table must have opened table handle.")
    }

    let table_meta = self.orchestrator.create_table_metadata(&name);
    meta_cursor.insert(name.as_bytes().to_vec(), table_meta.to_vec())?;

    let table = self.orchestrator.open_table(&table_meta)?;
    let cursor = Cursor::initialize(table.clone(), &self.context, self.metrics)?;
    self.orchestrator.register_table(table.clone());
    self.created_tables.push(table);

    Ok(cursor)
  }

  pub fn drop_table(&mut self, name: &str) -> Result {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }
    let name = TableName::from_str(name)?;

    let cursor = self.open_cursor(self.orchestrator.get_metadata_table(), None);
    let Some(bytes) = cursor.get(&name.as_bytes())? else {
      return Err(Error::TableNotFound(name.to_string()));
    };

    let metadata = TableMetadata::from_bytes(&bytes)?;
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
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }
    let name = TableName::from_str(name)?;
    let cursor = self.open_cursor(self.orchestrator.get_metadata_table(), None);

    let Some(bytes) = cursor.get(&name.as_bytes())? else {
      return Err(Error::TableNotFound(name.to_string()));
    };

    let mut metadata = TableMetadata::from_bytes(&bytes)?;
    if metadata.get_compaction_id().is_some() {
      return Ok(());
    }

    let Some(old) = self.orchestrator.get_table(metadata.get_id()) else {
      return Err(Error::TableNotFound(name.to_string()));
    };

    let table_meta = self.orchestrator.create_table_metadata(&name);
    metadata.set_compaction(&table_meta);

    if let Err(err) = cursor.insert(name.as_bytes().to_vec(), metadata.to_vec()) {
      if matches!(err, Error::WriteConflict) {
        return Ok(());
      }
      return Err(err);
    };

    let new_table = self
      .orchestrator
      .open_table(&table_meta)?
      .try_pin()
      .unwrap();

    Cursor::initialize(new_table.handle().clone(), &self.context, self.metrics)?;
    self.orchestrator.register_table(new_table.handle().clone());
    self.compacted_tables.push((old, new_table, table_meta));

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
    let version = self.context.state().current_version();

    let events = self
      .dropped_tables
      .drain(..)
      .map(|table| DropTableCommitted::new(table, id, version));
    self.context.event_bus().batch_publish(events);

    let events = self
      .compacted_tables
      .drain(..)
      .map(|(old, new, metadata)| CompactionCommitted::new(old, new, metadata, version));
    self.context.event_bus().batch_publish(events);
    self.created_tables.clear();

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
    self.orchestrator.abort_tx(id);
    state.deactive();

    self.clear();
    Ok(())
  }

  /**
   * Publish abort/drop cleanup for buffered table side effects.
   *
   * This method drains its buffers, so repeated calls are idempotent.
   */
  fn clear(&mut self) {
    let id = self.context.state().get_id();
    let version = self.context.state().current_version();

    let events = self
      .created_tables
      .drain(..)
      .map(|table| DropTableCommitted::new(table, id, version));
    self.context.event_bus().batch_publish(events);

    let events = self
      .compacted_tables
      .drain(..)
      .map(|(_, new, _)| DropTableCommitted::new(new.into_inner(), id, version));
    self.context.event_bus().batch_publish(events);
  }
}
impl<'a> Drop for Transaction<'a> {
  fn drop(&mut self) {
    let _ = self.abort();
    self.metrics.transaction_start.record(self.tx_start.take());
    self.clear();
  }
}
