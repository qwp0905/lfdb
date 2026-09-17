/**
 * Cursor over a logical table that may be split across compaction segments.
 *
 * Once compaction metadata is visible, new writes are routed to the compaction
 * segment and the old segment becomes read-only. Reads therefore check the
 * compaction segment first, and scans merge it as the primary stream over the
 * old segment.
 */
use std::ops::{Bound, RangeBounds};

use crate::{
  btree::{
    BTreeIndex, BTreeIter, BTreeRevIter, BulkOp, GetResult, LookupResult, MergeSortable,
    MergeSorted, SortDirection, WriteResult,
  },
  cache::VecRef,
  metrics::{measure, MetricsRegistry},
  objects::{StaticKey, StaticKeyRef, MAX_KEY, MAX_VALUE},
  table::TableHandleRef,
  Error, Result,
};

use super::TxContext;

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
  pub(crate) fn initialize(
    table: TableHandleRef,
    context: &'a TxContext<'a>,
    metrics: &'a MetricsRegistry,
  ) -> Result<Self> {
    let cursor = Self::new(table, None, context, metrics);
    cursor.index.initialize(&cursor.table)?;

    Ok(cursor)
  }

  pub(crate) const fn new(
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

  pub fn contains<K: AsRef<[u8]>>(&self, key: &K) -> Result<bool> {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }
    let key = key.as_ref();
    if key.len() > MAX_KEY {
      return Err(Error::KeyExceeded(MAX_KEY, key.len()));
    }

    if let Some(table) = self.compaction.as_ref() {
      match self.index.lookup(key, table)? {
        LookupResult::Absent => {}
        LookupResult::Deleted => return Ok(false),
        LookupResult::Present => return Ok(true),
      }
    }
    self.index.contains(key, &self.table)
  }

  fn __get(&self, key: StaticKeyRef) -> Result<Option<VecRef>> {
    if let Some(table) = self.compaction.as_ref() {
      match self.index.get(key, table)? {
        GetResult::Absent => {}
        GetResult::Deleted => return Ok(None),
        GetResult::Present(bytes) => return Ok(Some(bytes)),
      }
    }

    Ok(match self.index.get(key, &self.table)? {
      GetResult::Present(bytes) => Some(bytes),
      _ => None,
    })
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

    let result = measure!(self.metrics.operation_insert, self.__insert(key, value))?;
    if result.splitted {
      self.metrics.btree_split.inc();
    }
    Ok(())
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
      return self.index.remove(key, table);
    }
    self.index.remove_if_matched(key, &self.table)
  }
  pub fn remove<K: AsRef<[u8]>>(&self, key: &K) -> Result {
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
    Ok(())
  }

  pub fn range<'b, K>(
    &'a self,
    range: impl RangeBounds<&'b K>,
  ) -> Result<CursorIter<'a, BTreeIter<&'a &'a TxContext<'a>>>>
  where
    K: AsRef<[u8]> + ?Sized + 'b,
  {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }

    // Own range bounds inside the iterator so user-provided key references do not
    // extend into the cursor scan lifetime.
    CursorIter::new(
      self.context,
      &self.table,
      self.compaction.as_ref(),
      &self.index,
      range.start_bound().map(|k| k.as_ref().to_vec()),
      range.end_bound().map(|k| k.as_ref().to_vec()),
    )
  }
  pub fn range_rev<'b, K>(
    &'a self,
    range: impl RangeBounds<&'b K>,
  ) -> Result<CursorIter<'a, BTreeRevIter<&'a &'a TxContext<'a>>>>
  where
    K: AsRef<[u8]> + ?Sized + 'b,
  {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }

    // Own range bounds inside the iterator so user-provided key references do not
    // extend into the cursor scan lifetime.
    CursorIter::new_rev(
      self.context,
      &self.table,
      self.compaction.as_ref(),
      &self.index,
      range.start_bound().map(|k| k.as_ref().to_vec()),
      range.end_bound().map(|k| k.as_ref().to_vec()),
    )
  }

  pub fn create_bulk(&self) -> Bulk<'_> {
    let (table, in_compaction) = match &self.compaction {
      Some(table) => (table, true),
      None => (&self.table, false),
    };
    Bulk::new(&self.index, table, self.metrics, in_compaction)
  }
}

pub struct CursorIter<'a, Iter> {
  context: &'a TxContext<'a>,
  iter: MergeSorted<Iter>,
}
impl<'a> CursorIter<'a, BTreeIter<&'a &'a TxContext<'a>>> {
  pub(crate) fn new(
    context: &'a TxContext,
    table: &'a TableHandleRef,
    compaction: Option<&'a TableHandleRef>,
    index: &'a BTreeIndex<&'a TxContext<'a>>,
    start: Bound<StaticKey>,
    end: Bound<StaticKey>,
  ) -> Result<Self> {
    let default = index.range(table, &start, &end)?;
    let iter = match compaction {
      Some(c) => MergeSorted::merge(
        index.range(c, &start, &end)?,
        default,
        SortDirection::Ascending,
      ),
      None => MergeSorted::single(default, SortDirection::Ascending),
    };

    Ok(Self { context, iter })
  }
}
impl<'a> CursorIter<'a, BTreeRevIter<&'a &'a TxContext<'a>>> {
  pub(crate) fn new_rev(
    context: &'a TxContext,
    table: &'a TableHandleRef,
    compaction: Option<&'a TableHandleRef>,
    index: &'a BTreeIndex<&'a TxContext<'a>>,
    start: Bound<StaticKey>,
    end: Bound<StaticKey>,
  ) -> Result<Self> {
    let default = index.range_rev(table, &start, &end)?;
    let iter = match compaction {
      Some(c) => MergeSorted::merge(
        index.range_rev(c, &start, &end)?,
        default,
        SortDirection::Descending,
      ),
      None => MergeSorted::single(default, SortDirection::Descending),
    };

    Ok(Self { context, iter })
  }
}
impl<'a, Iter: MergeSortable> CursorIter<'a, Iter> {
  pub fn try_next(&mut self) -> Result<Option<(VecRef, VecRef)>> {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }

    self.iter.get_next_pair()
  }
}

pub struct Bulk<'a> {
  index: &'a BTreeIndex<&'a TxContext<'a>>,
  table: &'a TableHandleRef,
  metrics: &'a MetricsRegistry,
  inner: BulkOp,
  in_compaction: bool,
}
impl<'a> Bulk<'a> {
  const fn new(
    index: &'a BTreeIndex<&'a TxContext<'a>>,
    table: &'a TableHandleRef,
    metrics: &'a MetricsRegistry,
    in_compaction: bool,
  ) -> Self {
    Self {
      index,
      table,
      metrics,
      inner: BulkOp::new(),
      in_compaction,
    }
  }

  pub fn insert(&mut self, key: StaticKey, value: Vec<u8>) -> &mut Self {
    self.inner.append_insert(key, value, true);
    self
  }

  pub fn remove(&mut self, key: StaticKey) -> &mut Self {
    self.inner.append_remove(key, self.in_compaction);
    self
  }

  pub fn execute(self) -> Result {
    let mut executor = self.index.bulk_executor(self.inner, self.table);
    while let Some(result) = executor.drain_once()? {
      for _ in 0..result.splitted {
        self.metrics.btree_split.inc();
      }
    }
    Ok(())
  }
}
