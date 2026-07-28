/**
 * Cursor over a logical table that may be split across compaction segments.
 *
 * Once compaction metadata is visible, new writes are routed to the compaction
 * segment and the old segment becomes read-only. Reads therefore check the
 * compaction segment first, and scans merge it as the primary stream over the
 * old segment.
 */
use std::ops::{Bound, RangeBounds};

use super::{
  BTreeIndex, BTreeIterator, MergeSortable, MergeSorted, VecRef, WriteOp, WriteResult,
};
use crate::{
  measure,
  metrics::MetricsRegistry,
  objects::{StaticKey, StaticKeyRef, MAX_KEY, MAX_VALUE},
  table::TableHandleRef,
  transaction::TxContext,
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

  fn __get(&self, key: StaticKeyRef) -> Result<Option<VecRef>> {
    if let Some(table) = self.compaction.as_ref() {
      if let Some(found) = self.index.get(key, table)? {
        return Ok(found);
      }
    }

    Ok(self.index.get(key, &self.table)?.flatten())
  }
  pub fn get<K: AsRef<[u8]>>(&self, key: &K) -> Result<Option<VecRef>> {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }
    let key = key.as_ref();
    if key.len() > MAX_KEY {
      return Err(Error::KeyExceeded(MAX_KEY, key.len()));
    }

    measure!(self.metrics.operation_get, self.__get(key))
  }

  fn __insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<WriteResult> {
    let table = self.compaction.as_ref().unwrap_or(&self.table);
    self.index.insert(key, value, table)
  }
  pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<InsertResult> {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }

    if key.len() > MAX_KEY {
      return Err(Error::KeyExceeded(MAX_KEY, key.len()));
    }
    if value.len() > MAX_VALUE {
      return Err(Error::ValueExceeded(MAX_VALUE, value.len()));
    }

    let result = measure!(self.metrics.operation_insert, self.__insert(key, value))?;
    if result.splitted {
      self.metrics.btree_split.inc();
    }
    Ok(InsertResult {
      updated: result.updated,
      inserted: result.inserted,
    })
  }

  /**
   * During compaction, removal is written as a tombstone in the new segment.
   *
   * The old segment may still contain the key and the compaction copy may not have
   * reached it yet. Since reads merge both segments with the new segment first,
   * the tombstone must exist in the new segment to shadow the old value.
   */
  fn __remove(&self, key: StaticKeyRef) -> Result<WriteResult> {
    if let Some(table) = self.compaction.as_ref() {
      return self
        .index
        .insert_record(key.to_vec(), WriteOp::Remove, table);
    }
    self.index.remove(key, &self.table)
  }
  pub fn remove<K: AsRef<[u8]>>(&self, key: &K) -> Result<RemoveResult> {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }
    let key = key.as_ref();
    if key.len() > MAX_KEY {
      return Err(Error::KeyExceeded(MAX_KEY, key.len()));
    }

    let result = measure!(self.metrics.operation_remove, self.__remove(key))?;
    if result.splitted {
      self.metrics.btree_split.inc();
    }
    Ok(RemoveResult {
      removed: result.updated || result.inserted,
    })
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

    // Own range bounds inside the iterator so user-provided key references do not
    // extend into the cursor scan lifetime.
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

/**
 * Result of an insert operation.
 *
 * `updated` and `inserted` are logically exclusive; both are exposed so callers
 * can tell whether the write replaced an existing logical key or created a new
 * one.
 */
pub struct InsertResult {
  pub updated: bool,
  pub inserted: bool,
}

pub struct RemoveResult {
  pub removed: bool,
}
