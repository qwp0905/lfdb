use std::{
  marker::PhantomData,
  mem::replace,
  ops::RangeBounds,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
};

use super::{
  CursorIterator, CursorNode, CursorNodeView, DataChunk, DataEntry, InternalNode, Key,
  KeyRef, NodeFindResult, RecordData, TreeHeader, VersionRecord, CHUNK_SIZE,
  HEADER_POINTER, LARGE_VALUE, MAX_KEY, MAX_VALUE,
};
use crate::{
  buffer_pool::{ReadonlySlot, WritableSlot},
  disk::Pointer,
  metrics::MetricsRegistry,
  serialize::Serializable,
  table::TableHandle,
  transaction::{TxOrchestrator, TxSnapshot, TxState},
  Error, Result,
};

/**
 * A handle for a single table, providing read and write operations.
 * Must be used on a single thread; cross-thread behavior is untested.
 */
pub struct Cursor<'a> {
  orchestrator: &'a TxOrchestrator,
  state: &'a TxState<'a>,
  snapshot: &'a TxSnapshot<'a>,
  table: Arc<TableHandle>,
  compaction: Option<Arc<TableHandle>>,
  metrics: &'a MetricsRegistry,
  modified: &'a AtomicBool,
  _marker: PhantomData<*const ()>,
}
impl<'a> Cursor<'a> {
  #[inline]
  fn alloc_and_log<T: Serializable>(
    &self,
    data: &T,
    table: &Arc<TableHandle>,
  ) -> Result<Pointer> {
    let slot = &mut self.orchestrator.alloc(table.clone())?;
    self.serialize_and_log(slot, data, table)?;
    Ok(slot.get_pointer())
  }
  #[inline]
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

  pub fn initialize(
    table: Arc<TableHandle>,
    orchestrator: &'a TxOrchestrator,
    state: &'a TxState<'a>,
    snapshot: &'a TxSnapshot<'a>,
    modified: &'a AtomicBool,
    metrics: &'a MetricsRegistry,
  ) -> Result<Self> {
    let cursor = Self {
      table: table.clone(),
      orchestrator,
      state,
      snapshot,
      metrics,
      modified,
      compaction: None,
      _marker: PhantomData,
    };
    let root = cursor.alloc_and_log(&CursorNode::initial_state(), &table)?;
    let mut header = orchestrator
      .fetch(HEADER_POINTER, table.clone())?
      .for_write();
    cursor.serialize_and_log(&mut header, &TreeHeader::new(root), &table)?;
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
    Self {
      table,
      orchestrator,
      state,
      snapshot,
      metrics,
      modified,
      compaction,
      _marker: PhantomData,
    }
  }

  fn get_entry(
    &self,
    key: KeyRef,
    table: &Arc<TableHandle>,
  ) -> Result<Option<(ReadonlySlot<'a>, Pointer)>> {
    let mut ptr = self
      .orchestrator
      .fetch(HEADER_POINTER, table.clone())?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    loop {
      let mut slot = self.orchestrator.fetch(ptr, table.clone())?.for_read();

      match slot.as_ref().view::<CursorNodeView>()? {
        CursorNodeView::Internal(node) => ptr = node.find(key).unwrap_or_else(|i| i),
        CursorNodeView::Leaf(mut node) => loop {
          match node.find(key) {
            NodeFindResult::Found(_, i) => return Ok(Some((slot, i))),
            NodeFindResult::Move(i) => {
              drop(slot);
              slot = self.orchestrator.fetch(i, table.clone())?.for_read();
              node = slot.as_ref().view::<CursorNodeView>()?.as_leaf()?;
            }
            NodeFindResult::NotFound(_) => return Ok(None),
          }
        },
      }
    }
  }

  fn __get(
    &self,
    key: KeyRef,
    table: &Arc<TableHandle>,
  ) -> Result<Option<Option<Vec<u8>>>> {
    let (mut slot, ptr) = match self.get_entry(key, table)? {
      Some(v) => v,
      None => return Ok(None),
    };

    let _ = replace(
      &mut slot,
      self.orchestrator.fetch(ptr, table.clone())?.for_read(),
    );

    loop {
      let entry: DataEntry = slot.as_ref().deserialize()?;
      if let Some(record) =
        entry.find_record(self.state.get_id(), |i| self.snapshot.is_visible(i))
      {
        return Ok(Some(record.read_data(|i| {
          self
            .orchestrator
            .fetch(i, table.clone())?
            .for_read()
            .as_ref()
            .deserialize()
        })?));
      }

      match entry.get_next() {
        Some(i) => drop(replace(
          &mut slot,
          self.orchestrator.fetch(i, table.clone())?.for_read(),
        )),
        None => return Ok(None),
      }
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
        if let Some(found) = self.__get(key.as_ref(), table)? {
          return Ok(found);
        }
      }

      Ok(self.__get(key.as_ref(), &self.table)?.flatten())
    })
  }

  fn create_record(
    &self,
    mut data: Vec<u8>,
    table: &Arc<TableHandle>,
  ) -> Result<RecordData> {
    if data.len() < LARGE_VALUE {
      return Ok(RecordData::Data(data));
    }

    let mut pointers = Vec::with_capacity(data.len().div_ceil(CHUNK_SIZE));
    while data.len() > CHUNK_SIZE {
      let remain = data.split_off(CHUNK_SIZE);
      let chunk = DataChunk::new(data);
      pointers.push(self.alloc_and_log(&chunk, table)?);
      data = remain;
    }
    pointers.push(self.alloc_and_log(&DataChunk::new(data), table)?);

    Ok(RecordData::Chunked(pointers))
  }

  fn __insert(&self, key: Key, record: RecordData, table: &Arc<TableHandle>) -> Result {
    let (mut ptr, mut old_height) = {
      let header = self
        .orchestrator
        .fetch(HEADER_POINTER, table.clone())?
        .for_read()
        .as_ref()
        .deserialize::<TreeHeader>()?;
      (header.get_root(), header.get_height())
    };
    let mut stack = vec![];

    while let CursorNodeView::Internal(node) = self
      .orchestrator
      .fetch(ptr, table.clone())?
      .for_read()
      .as_ref()
      .view::<CursorNodeView>()?
    {
      match node.find(&key) {
        Ok(i) => stack.push(replace(&mut ptr, i)),
        Err(i) => ptr = i,
      }
    }

    let (mid_key, right_ptr) = loop {
      let mut slot = self.orchestrator.fetch(ptr, table.clone())?.for_write();

      let node = slot.as_ref().view::<CursorNodeView>()?.as_leaf()?;
      match node.find(&key) {
        NodeFindResult::Found(_, i) => return self.insert_at(i, record, slot, table),
        NodeFindResult::Move(i) => ptr = i,
        NodeFindResult::NotFound(i) => {
          let mut node = node.writable();
          let entry = DataEntry::init(VersionRecord::new(
            self.state.get_id(),
            self.orchestrator.current_version(),
            record,
          ));
          let entry_ptr = self.alloc_and_log(&entry, table)?;

          let split = match node.insert_and_split(i, key.clone(), entry_ptr) {
            Some(split) => split,
            None => return self.serialize_and_log(&mut slot, &node.to_node(), table),
          };

          let mid_key = split.top().clone();
          let split_ptr = self.alloc_and_log(&split.to_node(), table)?;

          node.set_next(split_ptr);
          self.serialize_and_log(&mut slot, &node.to_node(), table)?;

          break (mid_key, split_ptr);
        }
      }
    };

    let mut split_key = mid_key;
    let mut split_pointer = right_ptr;
    while let Some(index) = stack.pop() {
      match self.apply_split(split_key, split_pointer, index, table)? {
        Some((k, p)) => {
          split_key = k;
          split_pointer = p;
        }
        None => return Ok(()),
      };
    }

    // CAS loop: multiple concurrent splits may race to update the root.
    loop {
      let mut header_slot = self
        .orchestrator
        .fetch(HEADER_POINTER, table.clone())?
        .for_write();
      let mut header: TreeHeader = header_slot.as_ref().deserialize()?;
      let current_height = header.get_height();
      let mut ptr = header.get_root();
      if old_height == current_height {
        let new_root = InternalNode::initialize(split_key, ptr, split_pointer);
        let new_root_index = self.alloc_and_log(&new_root.to_node(), table)?;

        header.set_root(new_root_index);
        header.increase_height();
        return self.serialize_and_log(&mut header_slot, &header, table);
      }

      let diff = (current_height - old_height) as usize;
      old_height = current_height;
      drop(header_slot);
      let mut stack = vec![];

      while stack.len() < diff {
        let slot = self.orchestrator.fetch(ptr, table.clone())?.for_read();
        let node = slot.as_ref().view::<CursorNodeView>()?.as_internal()?;
        match node.find(&split_key) {
          Ok(i) => stack.push(replace(&mut ptr, i)),
          Err(i) => ptr = i,
        }
      }

      while let Some(index) = stack.pop() {
        match self.apply_split(split_key, split_pointer, index, table)? {
          Some((k, p)) => {
            split_key = k;
            split_pointer = p;
          }
          None => return Ok(()),
        };
      }
    }
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
      .measure(|| self.__insert(key, self.create_record(value, table)?, table))
  }

  fn apply_split(
    &self,
    evicted_key: Key,
    evicted_ptr: Pointer,
    current: Pointer,
    table: &Arc<TableHandle>,
  ) -> Result<Option<(Key, Pointer)>> {
    let mut ptr = current;

    let (mut slot, mut internal) = loop {
      let slot = self.orchestrator.fetch(ptr, table.clone())?.for_write();
      let mut internal = slot.as_ref().deserialize::<CursorNode>()?.as_internal()?;
      match internal.insert_or_next(&evicted_key, evicted_ptr) {
        Ok(_) => break (slot, internal),
        Err(i) => ptr = i,
      }
    };

    let (split_node, split_key) = match internal.split_if_needed() {
      Some(split) => split,
      None => {
        return self
          .serialize_and_log(&mut slot, &internal.to_node(), table)
          .map(|_| None)
      }
    };

    let split_index = self.alloc_and_log(&split_node.to_node(), table)?;

    internal.set_right(&split_key, split_index);
    self.serialize_and_log(&mut slot, &internal.to_node(), table)?;

    Ok(Some((split_key, split_index)))
  }

  /**
   * coupling required because of gc can collect entry header before write lock.
   */
  fn insert_at<T>(
    &self,
    entry_ptr: Pointer,
    data: RecordData,
    coupling: T,
    table: &Arc<TableHandle>,
  ) -> Result {
    let mut slot = self
      .orchestrator
      .fetch(entry_ptr, table.clone())?
      .for_write();
    drop(coupling);

    let mut entry: DataEntry = slot.as_ref().deserialize()?;
    if let Some(owner) = entry.get_last_owner() {
      if owner != self.state.get_id() && self.snapshot.is_active(&owner) {
        return Err(Error::WriteConflict);
      }
    }

    self.orchestrator.mark_gc(table.clone(), entry_ptr);
    let version = self.orchestrator.current_version();
    let record = VersionRecord::new(self.state.get_id(), version, data);

    if entry.is_available(&record) {
      entry.append(record);
      return self.serialize_and_log(&mut slot, &entry, table);
    }

    let new_entry_index = self.alloc_and_log(&entry, table)?;

    let mut new_entry = DataEntry::init(record);
    new_entry.set_next(new_entry_index);
    self.serialize_and_log(&mut slot, &new_entry, table)?;
    Ok(())
  }

  fn __remove(&self, key: KeyRef, table: &Arc<TableHandle>) -> Result {
    if let Some((slot, ptr)) = self.get_entry(key, table)? {
      self.insert_at(ptr, RecordData::Tombstone, slot, table)?;
    }
    Ok(())
  }

  pub fn remove<K: AsRef<[u8]>>(&self, key: &K) -> Result {
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
    }
    if key.as_ref().len() > MAX_KEY {
      return Err(Error::KeyExceeded(MAX_KEY, key.as_ref().len()));
    }

    if let Some(table) = self.compaction.as_ref() {
      return self
        .metrics
        .operation_remove
        .measure(|| self.__insert(key.as_ref().to_vec(), RecordData::Tombstone, table));
    }

    self
      .metrics
      .operation_remove
      .measure(|| self.__remove(key.as_ref(), &self.table))
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
      &self.table,
      self.compaction.as_ref(),
      &self.state,
      &self.snapshot,
      &self.orchestrator,
      range.start_bound().map(|v| v.as_ref().to_vec()),
      range.end_bound().map(|v| v.as_ref().to_vec()),
    )
  }
}
