use std::{
  cmp::Ordering,
  marker::PhantomData,
  ops::{Bound, RangeBounds},
  sync::Arc,
};

use super::{BTreeIndex, BTreeIterator, Key, RecordData, MAX_KEY, MAX_VALUE};
use crate::{
  metrics::MetricsRegistry, table::TableHandle, transaction::TxContext, Error, Result,
};

/**
 * A handle for a single table, providing read and write operations.
 * Must be used on a single thread; cross-thread behavior is untested.
 */
pub struct Cursor<'a> {
  context: &'a TxContext<'a>,
  index: BTreeIndex<&'a TxContext<'a>>,
  table: Arc<TableHandle>,
  compaction: Option<Arc<TableHandle>>,
  metrics: &'a MetricsRegistry,
  _marker: PhantomData<*const ()>,
}
impl<'a> Cursor<'a> {
  pub fn initialize(
    table: Arc<TableHandle>,
    context: &'a TxContext<'a>,
    metrics: &'a MetricsRegistry,
  ) -> Result<Self> {
    let cursor = Self::new(table, None, context, metrics);
    cursor.index.initialize(&cursor.table)?;

    Ok(cursor)
  }

  pub const fn new(
    table: Arc<TableHandle>,
    compaction: Option<Arc<TableHandle>>,
    context: &'a TxContext<'a>,
    metrics: &'a MetricsRegistry,
  ) -> Self {
    Self {
      context,
      index: BTreeIndex::new(context),
      table,
      metrics,
      compaction,
      _marker: PhantomData,
    }
  }

  pub fn get<K: AsRef<[u8]>>(&self, key: &K) -> Result<Option<Vec<u8>>> {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }
    if key.as_ref().len() > MAX_KEY {
      return Err(Error::KeyExceeded(MAX_KEY, key.as_ref().len()));
    }

    self.metrics.operation_get.measure(|| {
      if let Some(table) = self.compaction.as_ref() {
        if let Some(found) = self.index.get(key.as_ref(), table)? {
          return Ok(found);
        }
      }

      Ok(self.index.get(key.as_ref(), &self.table)?.flatten())
    })
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

    let table = self.compaction.as_ref().unwrap_or_else(|| &self.table);
    self
      .metrics
      .operation_insert
      .measure(|| self.index.insert(key, value, table))
      .map(|_| ())
  }

  pub fn remove<K: AsRef<[u8]>>(&self, key: &K) -> Result {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }
    if key.as_ref().len() > MAX_KEY {
      return Err(Error::KeyExceeded(MAX_KEY, key.as_ref().len()));
    }

    if let Some(table) = self.compaction.as_ref() {
      return self
        .metrics
        .operation_remove
        .measure(|| {
          self
            .index
            .insert_record(key.as_ref().to_vec(), RecordData::Tombstone, table)
        })
        .map(|_| ());
    }

    self
      .metrics
      .operation_remove
      .measure(|| self.index.remove(key.as_ref(), &self.table))
      .map(|_| ())
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
  table: BTreeIterator<'a, &'a TxContext<'a>>,
  compaction: Option<(
    BTreeIterator<'a, &'a TxContext<'a>>,
    Option<(Vec<u8>, Option<Vec<u8>>)>,
  )>,
  buffered: Option<(Vec<u8>, Option<Vec<u8>>)>,
}
impl<'a> CursorIterator<'a> {
  pub fn new(
    context: &'a TxContext,
    table: &'a Arc<TableHandle>,
    compaction: Option<&'a Arc<TableHandle>>,
    index: &'a BTreeIndex<&'a TxContext<'a>>,
    start: Bound<Key>,
    end: Bound<Key>,
  ) -> Result<Self> {
    let compaction = match compaction {
      Some(table) => Some((index.scan(table, &start, &end)?, None)),
      None => None,
    };

    Ok(Self {
      context,
      table: index.scan(table, &start, &end)?,
      compaction,
      buffered: None,
    })
  }
  pub fn try_next(&mut self) -> Result<Option<(Key, Vec<u8>)>> {
    if !self.context.is_available() {
      return Err(Error::TransactionClosed);
    }

    let (compaction, c_buffered) = match &mut self.compaction {
      Some(v) => v,
      None => loop {
        match self.table.next_kv()? {
          Some((_, None)) => continue,
          Some((k, Some(v))) => return Ok(Some((k, v))),
          None => return Ok(None),
        }
      },
    };

    loop {
      let kv_old = match self.buffered.take() {
        Some(kv) => Some(kv),
        None => self.table.next_kv()?,
      };
      let kv_new = match c_buffered.take() {
        Some(kv) => Some(kv),
        None => compaction.next_kv()?,
      };

      let (k_old, v_old, k_new, v_new) = match (kv_old, kv_new) {
        (None, None) => return Ok(None),
        (None, Some((k, Some(v)))) | (Some((k, Some(v))), None) => {
          return Ok(Some((k, v)))
        }
        (None, Some((_, None))) | (Some((_, None)), None) => continue,
        (Some((k1, v1)), Some((k2, v2))) => (k1, v1, k2, v2),
      };

      let (k, v) = match k_old.cmp(&k_new) {
        Ordering::Less => {
          *c_buffered = Some((k_new, v_new));
          (k_old, v_old)
        }
        Ordering::Greater => {
          self.buffered = Some((k_old, v_old));
          (k_new, v_new)
        }
        Ordering::Equal => (k_new, v_new),
      };
      if let Some(v) = v {
        return Ok(Some((k, v)));
      }
    }
  }
}
