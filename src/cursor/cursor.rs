use std::{
  marker::PhantomData,
  ops::{Bound, RangeBounds},
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
};

use super::{
  BTreeIndex, BTreeIterator, CreatablePolicy, Key, ReadonlyPolicy, RecordData,
  VersionRecord, WritablePolicy, MAX_KEY, MAX_VALUE,
};
use crate::{
  buffer_pool::WritableSlot,
  disk::Pointer,
  metrics::MetricsRegistry,
  serialize::Serializable,
  table::TableHandle,
  transaction::{TxOrchestrator, TxSnapshot, TxState},
  Error, Result,
};

pub struct CursorPolicy<'a> {
  orchestrator: &'a TxOrchestrator,
  state: &'a TxState<'a>,
  snapshot: &'a TxSnapshot<'a>,
  modified: &'a AtomicBool,
}
impl<'a> ReadonlyPolicy for CursorPolicy<'a> {
  fn is_visible(&self, owner: crate::wal::TxId, version: crate::wal::TxId) -> bool {
    let current = self.state.get_id();
    if owner == current {
      return true;
    }
    version <= current && self.snapshot.is_visible(&owner)
  }

  fn fetch_slot(
    &self,
    pointer: Pointer,
    table: &Arc<TableHandle>,
  ) -> Result<crate::buffer_pool::Slot<'_>> {
    self.orchestrator.fetch(pointer, table.clone())
  }
}
impl<'a> WritablePolicy for CursorPolicy<'a> {
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut WritableSlot<'_>,
    data: &T,
    table: &Arc<TableHandle>,
  ) -> Result {
    self.orchestrator.serialize_and_log(
      self.state.get_id(),
      table.metadata().get_id(),
      slot,
      data,
    )?;
    self.modified.fetch_or(true, Ordering::Release);
    Ok(())
  }

  fn after_update_entry(&self, pointer: Pointer, table: &Arc<TableHandle>) {
    self.orchestrator.mark_gc(table.clone(), pointer);
  }
}
impl<'a> CreatablePolicy for CursorPolicy<'a> {
  fn is_conflict(&self, owner: crate::wal::TxId) -> bool {
    owner != self.state.get_id() && self.snapshot.is_active(&owner)
  }
  fn create_record(&self, data: RecordData) -> VersionRecord {
    VersionRecord::new(
      self.state.get_id(),
      self.orchestrator.current_version(),
      data,
    )
  }
}

/**
 * A handle for a single table, providing read and write operations.
 * Must be used on a single thread; cross-thread behavior is untested.
 */
pub struct Cursor<'a> {
  state: &'a TxState<'a>,
  index: BTreeIndex<CursorPolicy<'a>>,
  table: Arc<TableHandle>,
  compaction: Option<Arc<TableHandle>>,
  metrics: &'a MetricsRegistry,
  _marker: PhantomData<*const ()>,
}
impl<'a> Cursor<'a> {
  pub fn initialize(
    table: Arc<TableHandle>,
    orchestrator: &'a TxOrchestrator,
    state: &'a TxState<'a>,
    snapshot: &'a TxSnapshot<'a>,
    modified: &'a AtomicBool,
    metrics: &'a MetricsRegistry,
  ) -> Result<Self> {
    let cursor = Self::new(
      table,
      None,
      orchestrator,
      state,
      snapshot,
      modified,
      metrics,
    );
    cursor.index.initialize(&cursor.table)?;

    Ok(cursor)
  }

  pub fn new(
    table: Arc<TableHandle>,
    compaction: Option<Arc<TableHandle>>,
    orchestrator: &'a TxOrchestrator,
    state: &'a TxState<'a>,
    snapshot: &'a TxSnapshot<'a>,
    modified: &'a AtomicBool,
    metrics: &'a MetricsRegistry,
  ) -> Self {
    let policy = CursorPolicy {
      orchestrator,
      state,
      snapshot,
      modified,
    };
    Self {
      index: BTreeIndex::new(policy),
      state,
      table,
      metrics,
      compaction,
      _marker: PhantomData,
    }
  }

  pub fn get<K: AsRef<[u8]>>(&self, key: &K) -> Result<Option<Vec<u8>>> {
    if !self.state.is_available() {
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
    if !self.state.is_available() {
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
  }

  pub fn remove<K: AsRef<[u8]>>(&self, key: &K) -> Result {
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
    }
    if key.as_ref().len() > MAX_KEY {
      return Err(Error::KeyExceeded(MAX_KEY, key.as_ref().len()));
    }

    if let Some(table) = self.compaction.as_ref() {
      return self.metrics.operation_remove.measure(|| {
        self
          .index
          .insert_record(key.as_ref().to_vec(), RecordData::Tombstone, table)
      });
    }

    self.metrics.operation_remove.measure(|| {
      self.index.insert_record_if_matched(
        key.as_ref(),
        RecordData::Tombstone,
        &self.table,
      )
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
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
    }

    CursorIterator::new(
      &self.state,
      &self.table,
      self.compaction.as_ref(),
      &self.index,
      range.start_bound().map(|k| k.as_ref().to_vec()),
      range.end_bound().map(|k| k.as_ref().to_vec()),
    )
  }
}

pub struct CursorIterator<'a> {
  state: &'a TxState<'a>,
  table: BTreeIterator<'a, CursorPolicy<'a>>,
  compaction: Option<(
    BTreeIterator<'a, CursorPolicy<'a>>,
    Option<(Vec<u8>, Option<Vec<u8>>)>,
  )>,
  buffered: Option<(Vec<u8>, Option<Vec<u8>>)>,
}
impl<'a> CursorIterator<'a> {
  pub fn new(
    state: &'a TxState<'a>,
    table: &'a Arc<TableHandle>,
    compaction: Option<&'a Arc<TableHandle>>,
    index: &'a BTreeIndex<CursorPolicy<'a>>,
    start: Bound<Key>,
    end: Bound<Key>,
  ) -> Result<Self> {
    let mut compaction_table = None;
    if let Some(table) = compaction {
      compaction_table = Some((index.scan(table, &start, &end)?, None));
    }

    Ok(Self {
      state,
      table: index.scan(table, &start, &end)?,
      compaction: compaction_table,
      buffered: None,
    })
  }
  pub fn try_next(&mut self) -> Result<Option<(Key, Vec<u8>)>> {
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
    }

    let (compaction, cb) = match &mut self.compaction {
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
      let kv_new = match cb.take() {
        Some(kv) => Some(kv),
        None => compaction.next_kv()?,
      };

      let (k_old, v_old, k_new, v_new) = match (kv_old, kv_new) {
        (None, None) => return Ok(None),
        (None, Some((k, Some(v)))) => return Ok(Some((k, v))),
        (Some((k, Some(v))), None) => return Ok(Some((k, v))),
        (None, Some((_, None))) | (Some((_, None)), None) => continue,
        (Some((k1, v1)), Some((k2, v2))) => (k1, v1, k2, v2),
      };

      if k_old < k_new {
        *cb = Some((k_new, v_new));
        if let Some(v) = v_old {
          return Ok(Some((k_old, v)));
        }
        continue;
      }
      if k_new < k_old {
        self.buffered = Some((k_old, v_old));
        if let Some(v) = v_new {
          return Ok(Some((k_new, v)));
        }
        continue;
      }

      if let Some(v) = v_new {
        return Ok(Some((k_new, v)));
      }
    }
  }
}
