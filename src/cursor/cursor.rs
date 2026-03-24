use std::{marker::PhantomData, mem::replace, sync::Arc};

use super::{
  CursorIterator, CursorNode, DataEntry, NodeFindResult, RecordData, SplitTask,
  TreeHeader, VersionRecord, HEADER_INDEX,
};
use crate::{
  buffer_pool::WritableSlot,
  serialize::Serializable,
  transaction::{TxOrchestrator, TxState},
  Error, Result,
};

pub struct Cursor<'a> {
  orchestrator: Arc<TxOrchestrator>,
  state: TxState<'a>,
  _marker: PhantomData<*const ()>, // do not send to another thread!!!.
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

  pub fn new(orchestrator: Arc<TxOrchestrator>, state: TxState<'a>) -> Self {
    Self {
      orchestrator,
      state,
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

  pub fn get<K: AsRef<[u8]>>(&self, key: &K) -> Result<Option<Vec<u8>>> {
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
    }

    let mut index = self.find_leaf(key.as_ref())?;
    loop {
      let node = self
        .orchestrator
        .fetch(index)?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
        .as_leaf()?;
      match node.find(key.as_ref()) {
        NodeFindResult::Found(_, i) => break index = i,
        NodeFindResult::Move(i) => index = i,
        NodeFindResult::NotFound(_) => return Ok(None),
      }
    }

    let mut slot = self.orchestrator.fetch(index)?.for_read();
    loop {
      let entry: DataEntry = slot.as_ref().deserialize()?;
      if let Some(v) =
        entry.find_value(self.state.get_id(), |i| self.orchestrator.is_visible(i))
      {
        return Ok(Some(v));
      }

      match entry.get_next() {
        Some(i) => drop(replace(&mut slot, self.orchestrator.fetch(i)?.for_read())),
        None => return Ok(None),
      }
    }
  }

  pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result {
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
    }

    let (mut index, old_height) = {
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
        NodeFindResult::Found(_, i) => {
          return self.insert_at(i, RecordData::Data(value), slot)
        }
        NodeFindResult::NotFound(i) => {
          let entry = DataEntry::init(VersionRecord::new(
            self.state.get_id(),
            self.orchestrator.current_version(),
            RecordData::Data(value),
          ));
          let entry_index = self.alloc_and_log(&entry)?;

          let split = match leaf.insert_at(i, key.clone(), entry_index) {
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

    let task = SplitTask::new(stack, mid_key, right_ptr, old_height);
    self.orchestrator.split(task);
    Ok(())
  }

  /**
   * coupling required because of gc can collect entry header before write lock.
   */
  fn insert_at<T>(&self, entry_index: usize, data: RecordData, coupling: T) -> Result {
    let mut slot = self.orchestrator.fetch(entry_index)?.for_write();
    drop(coupling);

    let mut entry: DataEntry = slot.as_ref().deserialize()?;
    if let Some(owner) = entry.get_last_owner() {
      if owner != self.state.get_id() && self.orchestrator.is_active(&owner) {
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

  pub fn remove<K: AsRef<[u8]>>(&self, key: &K) -> Result {
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
    }
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

  pub fn scan<K: AsRef<[u8]>>(&self, start: &K, end: &K) -> Result<CursorIterator<'_>> {
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
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
      &self.orchestrator,
      leaf,
      0,
      None,
    ))
  }
}
impl<'a> Drop for Cursor<'a> {
  fn drop(&mut self) {
    let _ = self.abort();
  }
}
