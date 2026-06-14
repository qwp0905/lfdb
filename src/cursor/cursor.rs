use std::ops::{Bound, RangeBounds};

use super::{
  BTreeIndex, BTreeIterator, MergeSortable, MergeSorted, StaticKey, VecRef, MAX_KEY,
  MAX_VALUE,
};
use crate::{
  measure, metrics::MetricsRegistry, table::TableHandleRef, transaction::TxContext,
  Error, Result,
};

/**
 * A handle for a single table, providing read and write operations.
 */
pub struct Cursor<'a> {
  context: &'a TxContext<'a>,
  index: BTreeIndex<&'a TxContext<'a>>,
  table: TableHandleRef,
  compaction: Option<TableHandleRef>,
  metrics: &'a MetricsRegistry,
}
impl<'a> Cursor<'a> {
  pub fn initialize(
    table: TableHandleRef,
    context: &'a TxContext<'a>,
    metrics: &'a MetricsRegistry,
  ) -> Result<Self> {
    let cursor = Self::new(table, None, context, metrics);
    cursor.index.initialize(&cursor.table)?;

    Ok(cursor)
  }

  pub const fn new(
    table: TableHandleRef,
    compaction: Option<TableHandleRef>,
    context: &'a TxContext<'a>,
    metrics: &'a MetricsRegistry,
  ) -> Self {
    Self {
      context,
      index: BTreeIndex::new(context),
      table,
      metrics,
      compaction,
    }
  }

  pub fn get<K: AsRef<[u8]>>(&self, key: &K) -> Result<Option<VecRef>> {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }
    let key = key.as_ref();
    if key.len() > MAX_KEY {
      return Err(Error::KeyExceeded(MAX_KEY, key.len()));
    }

    let metrics = &self.metrics.operation_get;

    if let Some(table) = self.compaction.as_ref() {
      if let Some(found) = measure!(metrics, self.index.get(key, table))? {
        return Ok(found);
      }
    }

    measure!(metrics, Ok(self.index.get(key, &self.table)?.flatten()))
  }

  pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }

    if key.len() > MAX_KEY {
      return Err(Error::KeyExceeded(MAX_KEY, key.len()));
    }
    if value.len() > MAX_VALUE {
      return Err(Error::ValueExceeded(MAX_VALUE, value.len()));
    }

    let table = self.compaction.as_ref().unwrap_or(&self.table);
    measure!(
      self.metrics.operation_insert,
      self.index.insert(key, value, table)
    )?;
    Ok(())
  }

  pub fn remove<K: AsRef<[u8]>>(&self, key: &K) -> Result {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }
    let key = key.as_ref();
    if key.len() > MAX_KEY {
      return Err(Error::KeyExceeded(MAX_KEY, key.len()));
    }

    let metrics = &self.metrics.operation_remove;
    if let Some(table) = self.compaction.as_ref() {
      measure!(metrics, self.index.insert_record(key.to_vec(), None, table))?;
      return Ok(());
    }

    measure!(metrics, self.index.remove(key, &self.table))?;
    Ok(())
  }

  pub fn scan<'b, 'c, K>(
    &'a self,
    range: impl RangeBounds<&'c K>,
  ) -> Result<CursorIterator<'b>>
  where
    'a: 'b,
    K: AsRef<[u8]> + ?Sized + 'c,
  {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }

    CursorIterator::new(
      self.context,
      &self.table,
      self.compaction.as_ref(),
      &self.index,
      range.start_bound().map(|k| k.as_ref().to_vec()),
      range.end_bound().map(|k| k.as_ref().to_vec()),
    )
  }
}

pub struct CursorIterator<'a> {
  context: &'a TxContext<'a>,
  iter: MergeSorted<BTreeIterator<&'a &'a TxContext<'a>>>,
}
impl<'a> CursorIterator<'a> {
  pub fn new(
    context: &'a TxContext,
    table: &'a TableHandleRef,
    compaction: Option<&'a TableHandleRef>,
    index: &'a BTreeIndex<&'a TxContext<'a>>,
    start: Bound<StaticKey>,
    end: Bound<StaticKey>,
  ) -> Result<Self> {
    let iter = match compaction {
      Some(c) => MergeSorted::merge(
        index.scan(c, &start, &end)?,
        index.scan(table, &start, &end)?,
      ),
      None => MergeSorted::single(index.scan(table, &start, &end)?),
    };

    Ok(Self { context, iter })
  }
  pub fn try_next(&mut self) -> Result<Option<(VecRef, VecRef)>> {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }

    self.iter.get_next_pair()
  }
}
