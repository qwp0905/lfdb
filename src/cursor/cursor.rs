use std::{marker::PhantomData, mem::replace, sync::Arc, time::Instant};

use super::{
  CursorIterator, CursorNode, DataChunk, DataEntry, InternalNode, NodeFindResult,
  RecordData, TreeHeader, VersionRecord, CHUNK_SIZE, HEADER_INDEX, LARGE_VALUE, MAX_KEY,
  MAX_VALUE,
};
use crate::{
  buffer_pool::WritableSlot,
  metrics::MetricsRegistry,
  serialize::Serializable,
  transaction::{TxOrchestrator, TxSnapshot, TxState},
  Error, Result,
};

/**
 * A handle for a single transaction, providing read and write operations.
 * Must be used on a single thread; cross-thread behavior is untested.
 * Automatically aborts on drop if not committed.
 */
pub struct Cursor<'a> {
  orchestrator: Arc<TxOrchestrator>,
  state: TxState<'a>,
  snapshot: TxSnapshot<'a>,
  metrics: Arc<MetricsRegistry>,
  tx_start: Option<Instant>,
  _marker: PhantomData<*const ()>,
}
impl<'a> Cursor<'a> {
  #[inline]
  fn alloc_and_log<T: Serializable>(&self, data: &T) -> Result<usize> {
    let slot = &mut self.orchestrator.alloc()?;
    self.serialize_and_log(slot, data)?;
    Ok(slot.get_index())
  }
  #[inline]
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut WritableSlot<'_>,
    data: &T,
  ) -> Result {
    self
      .orchestrator
      .serialize_and_log(self.state.get_id(), slot, data)
  }

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
      _marker: Default::default(),
    }
  }

  pub fn commit(&mut self) -> Result {
    if !self.state.try_commit() {
      return Err(Error::TransactionClosed);
    }
    if let Err(err) = self.orchestrator.commit_tx(self.state.get_id()) {
      self.state.make_available();
      return Err(err);
    }

    self.state.complete_commit();
    self.state.deactive();
    Ok(())
  }

  pub fn abort(&mut self) -> Result {
    if !self.state.try_abort() {
      return Err(Error::TransactionClosed);
    }
    self.orchestrator.abort_tx(self.state.get_id())?;
    self.state.deactive();
    Ok(())
  }

  fn find_leaf(&self, key: &[u8]) -> Result<usize> {
    let mut index = self
      .orchestrator
      .fetch(HEADER_INDEX)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    while let CursorNode::Internal(node) = self
      .orchestrator
      .fetch(index)?
      .for_read()
      .as_ref()
      .deserialize()?
    {
      index = node.find(key).unwrap_or_else(|i| i);
    }
    Ok(index)
  }

  fn __get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let mut index = self.find_leaf(key)?;
    loop {
      let node = self
        .orchestrator
        .fetch(index)?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
        .as_leaf()?;
      match node.find(key) {
        NodeFindResult::Found(_, i) => break index = i,
        NodeFindResult::Move(i) => index = i,
        NodeFindResult::NotFound(_) => return Ok(None),
      }
    }

    let mut slot = self.orchestrator.fetch(index)?.for_read();
    loop {
      let entry: DataEntry = slot.as_ref().deserialize()?;
      if let Some(record) =
        entry.find_record(self.state.get_id(), |i| self.snapshot.is_visible(i))
      {
        return record.read_data(|i| {
          self
            .orchestrator
            .fetch(i)?
            .for_read()
            .as_ref()
            .deserialize()
        });
      }

      match entry.get_next() {
        Some(i) => drop(replace(&mut slot, self.orchestrator.fetch(i)?.for_read())),
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

    self
      .metrics
      .operation_get
      .measure(|| self.__get(key.as_ref()))
  }

  fn create_record(&self, mut data: Vec<u8>) -> Result<RecordData> {
    if data.len() < LARGE_VALUE {
      return Ok(RecordData::Data(data));
    }

    let mut pointers = Vec::with_capacity(data.len().div_ceil(CHUNK_SIZE));
    while data.len() > CHUNK_SIZE {
      let remain = data.split_off(CHUNK_SIZE);
      let chunk = DataChunk::new(data);
      pointers.push(self.alloc_and_log(&chunk)?);
      data = remain;
    }
    pointers.push(self.alloc_and_log(&DataChunk::new(data))?);

    Ok(RecordData::Chunked(pointers))
  }

  fn __insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result {
    let record = self.create_record(value)?;

    let (mut index, mut old_height) = {
      let header = self
        .orchestrator
        .fetch(HEADER_INDEX)?
        .for_read()
        .as_ref()
        .deserialize::<TreeHeader>()?;
      (header.get_root(), header.get_height())
    };
    let mut stack = vec![];

    while let CursorNode::Internal(node) = self
      .orchestrator
      .fetch(index)?
      .for_read()
      .as_ref()
      .deserialize()?
    {
      match node.find(&key) {
        Ok(i) => stack.push(replace(&mut index, i)),
        Err(i) => index = i,
      }
    }

    let (mid_key, right_ptr) = loop {
      let mut slot = self.orchestrator.fetch(index)?.for_write();
      let mut leaf = slot.as_ref().deserialize::<CursorNode>()?.as_leaf()?;

      match leaf.find(&key) {
        NodeFindResult::Move(i) => {
          index = i;
          continue;
        }
        NodeFindResult::Found(_, i) => return self.insert_at(i, record, slot),
        NodeFindResult::NotFound(i) => {
          let entry = DataEntry::init(VersionRecord::new(
            self.state.get_id(),
            self.orchestrator.current_version(),
            record,
          ));
          let entry_index = self.alloc_and_log(&entry)?;

          let split = match leaf.insert_and_split(i, key.clone(), entry_index) {
            Some(split) => split,
            None => return self.serialize_and_log(&mut slot, &leaf.to_node()),
          };

          let mid_key = split.top().clone();
          let split_index = self.alloc_and_log(&split.to_node())?;

          leaf.set_next(split_index);
          self.serialize_and_log(&mut slot, &leaf.to_node())?;

          break (mid_key, split_index);
        }
      }
    };

    let mut split_key = mid_key;
    let mut split_pointer = right_ptr;
    while let Some(index) = stack.pop() {
      match self.apply_split(split_key, split_pointer, index)? {
        Some((k, p)) => {
          split_key = k;
          split_pointer = p;
        }
        None => return Ok(()),
      };
    }

    // CAS loop: multiple concurrent splits may race to update the root.
    loop {
      let mut header_slot = self.orchestrator.fetch(HEADER_INDEX)?.for_write();
      let mut header: TreeHeader = header_slot.as_ref().deserialize()?;
      let current_height = header.get_height();
      let mut index = header.get_root();
      if old_height == current_height {
        let new_root = InternalNode::initialize(split_key, index, split_pointer);
        let new_root_index = self.alloc_and_log(&new_root.to_node())?;

        header.set_root(new_root_index);
        header.increase_height();
        return self.serialize_and_log(&mut header_slot, &header);
      }

      let diff = (current_height - old_height) as usize;
      old_height = current_height;
      drop(header_slot);
      let mut stack = vec![];

      while stack.len() < diff {
        let node = self
          .orchestrator
          .fetch(index)?
          .for_read()
          .as_ref()
          .deserialize::<CursorNode>()?
          .as_internal()?;
        match node.find(&split_key) {
          Ok(i) => stack.push(replace(&mut index, i)),
          Err(i) => index = i,
        }
      }

      while let Some(index) = stack.pop() {
        match self.apply_split(split_key, split_pointer, index)? {
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

    self
      .metrics
      .operation_insert
      .measure(|| self.__insert(key, value))
  }

  fn apply_split(
    &self,
    key: Vec<u8>,
    ptr: usize,
    current: usize,
  ) -> Result<Option<(Vec<u8>, usize)>> {
    let mut index = current;

    let (mut slot, mut internal) = loop {
      let slot = self.orchestrator.fetch(index)?.for_write();
      let mut internal = slot.as_ref().deserialize::<CursorNode>()?.as_internal()?;
      match internal.insert_or_next(&key, ptr) {
        Ok(_) => break (slot, internal),
        Err(i) => index = i,
      }
    };

    let (split_node, split_key) = match internal.split_if_needed() {
      Some(split) => split,
      None => {
        return self
          .serialize_and_log(&mut slot, &internal.to_node())
          .map(|_| None)
      }
    };

    let split_index = self.alloc_and_log(&split_node.to_node())?;

    internal.set_right(&split_key, split_index);
    self.serialize_and_log(&mut slot, &internal.to_node())?;

    Ok(Some((split_key, split_index)))
  }

  /**
   * coupling required because of gc can collect entry header before write lock.
   */
  fn insert_at<T>(&self, entry_index: usize, data: RecordData, coupling: T) -> Result {
    let mut slot = self.orchestrator.fetch(entry_index)?.for_write();
    drop(coupling);

    let mut entry: DataEntry = slot.as_ref().deserialize()?;
    if let Some(owner) = entry.get_last_owner() {
      if owner != self.state.get_id() && self.snapshot.is_active(&owner) {
        return Err(Error::WriteConflict);
      }
    }

    self.orchestrator.mark_gc(entry_index);
    let version = self.orchestrator.current_version();
    let record = VersionRecord::new(self.state.get_id(), version, data);

    if entry.is_available(&record) {
      entry.append(record);
      return self.serialize_and_log(&mut slot, &entry);
    }

    let new_entry_index = self.alloc_and_log(&entry)?;

    let mut new_entry = DataEntry::init(record);
    new_entry.set_next(new_entry_index);
    self.serialize_and_log(&mut slot, &new_entry)?;
    Ok(())
  }

  fn __remove(&self, key: &[u8]) -> Result {
    let mut index = self.find_leaf(key.as_ref())?;
    loop {
      let slot = self.orchestrator.fetch(index)?.for_read();
      let node = slot.as_ref().deserialize::<CursorNode>()?.as_leaf()?;
      match node.find(key.as_ref()) {
        NodeFindResult::Found(_, i) => {
          return self.insert_at(i, RecordData::Tombstone, slot)
        }
        NodeFindResult::Move(i) => index = i,
        NodeFindResult::NotFound(_) => return Ok(()),
      }
    }
  }

  pub fn remove<K: AsRef<[u8]>>(&self, key: &K) -> Result {
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
    }
    if key.as_ref().len() > MAX_KEY {
      return Err(Error::KeyExceeded(MAX_KEY, key.as_ref().len()));
    }

    self
      .metrics
      .operation_remove
      .measure(|| self.__remove(key.as_ref()))
  }

  pub fn scan<K: AsRef<[u8]>>(&self, start: &K, end: &K) -> Result<CursorIterator<'_>> {
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
    }
    if start.as_ref().len() > MAX_KEY {
      return Err(Error::KeyExceeded(MAX_KEY, start.as_ref().len()));
    }
    if end.as_ref().len() > MAX_KEY {
      return Err(Error::KeyExceeded(MAX_KEY, end.as_ref().len()));
    }

    let mut index = self.find_leaf(start.as_ref())?;
    let (leaf, pos) = loop {
      let node = self
        .orchestrator
        .fetch(index)?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
        .as_leaf()?;
      match node.find(start.as_ref()) {
        NodeFindResult::Found(pos, _) => break (node, pos),
        NodeFindResult::Move(i) => index = i,
        NodeFindResult::NotFound(pos) => break (node, pos),
      }
    };

    Ok(CursorIterator::new(
      &self.state,
      &self.snapshot,
      &self.orchestrator,
      leaf,
      pos,
      Some(end.as_ref().into()),
    ))
  }

  pub fn scan_all(&self) -> Result<CursorIterator<'_>> {
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
    }

    let mut index = self
      .orchestrator
      .fetch(HEADER_INDEX)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();
    let leaf = loop {
      let node: CursorNode = self
        .orchestrator
        .fetch(index)?
        .for_read()
        .as_ref()
        .deserialize()?;
      match node {
        CursorNode::Internal(internal) => index = internal.first_child(),
        CursorNode::Leaf(leaf) => break leaf,
      };
    };

    Ok(CursorIterator::new(
      &self.state,
      &self.snapshot,
      &self.orchestrator,
      leaf,
      0,
      None,
    ))
  }
}
impl<'a> Drop for Cursor<'a> {
  fn drop(&mut self) {
    self.metrics.transaction_start.record(self.tx_start.take());
    let _ = self.abort();
  }
}
