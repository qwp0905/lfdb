use std::{collections::VecDeque, mem::replace, ops::Bound, sync::Arc};

use crate::{
  cache::{ReadonlySlot, WritableSlot},
  disk::Pointer,
  table::TableHandle,
  wal::TxId,
  Error, Result,
};

use super::{
  BTreeNode, BTreeNodeView, CreatablePolicy, DataChunk, DataEntry, InternalNode, Key,
  KeyRef, LeafNode, NodeFindResult, ReadonlyPolicy, RecordData, TreeHeader,
  VersionRecord, WritablePolicy, CHUNK_SIZE, HEADER_POINTER, LARGE_VALUE,
};

pub struct BTreeIndex<Policy>(Policy);
impl<Policy> BTreeIndex<Policy> {
  pub fn new(policy: Policy) -> Self {
    Self(policy)
  }
}
impl<Policy: ReadonlyPolicy> BTreeIndex<Policy> {
  fn get_entry(
    &self,
    key: KeyRef,
    table: &Arc<TableHandle>,
  ) -> Result<Option<(ReadonlySlot<'_>, Pointer)>> {
    let mut ptr = self
      .0
      .fetch_slot(HEADER_POINTER, table)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    loop {
      let mut slot = self.0.fetch_slot(ptr, table)?.for_read();

      match slot.as_ref().view::<BTreeNodeView>()? {
        BTreeNodeView::Internal(node) => ptr = node.find(key).unwrap_or_else(|i| i),
        BTreeNodeView::Leaf(mut node) => loop {
          match node.find(key) {
            NodeFindResult::Found(_, i) => return Ok(Some((slot, i))),
            NodeFindResult::Move(i) => {
              let _ = replace(&mut slot, self.0.fetch_slot(i, table)?.for_read());
              node = slot.as_ref().view::<BTreeNodeView>()?.as_leaf()?;
            }
            NodeFindResult::NotFound(_) => return Ok(None),
          }
        },
      }
    }
  }

  pub fn get(
    &self,
    key: KeyRef,
    table: &Arc<TableHandle>,
  ) -> Result<Option<Option<Vec<u8>>>> {
    let (mut slot, ptr) = match self.get_entry(key, table)? {
      Some(v) => v,
      None => return Ok(None),
    };

    let _ = replace(&mut slot, self.0.fetch_slot(ptr, table)?.for_read());

    loop {
      let entry: DataEntry = slot.as_ref().deserialize()?;

      for record in entry.get_versions() {
        if self.0.is_visible(record.owner, record.version) {
          return Ok(Some(
            record
              .data
              .read_data(|i| self.0.fetch_and_deserialize(i, table))?,
          ));
        }
      }

      match entry.get_next() {
        Some(i) => drop(replace(&mut slot, self.0.fetch_slot(i, table)?.for_read())),
        None => return Ok(None),
      }
    }
  }

  pub fn contains(&self, key: KeyRef, table: &Arc<TableHandle>) -> Result<bool> {
    let (mut slot, ptr) = match self.get_entry(key, table)? {
      Some(v) => v,
      None => return Ok(false),
    };

    let _ = replace(&mut slot, self.0.fetch_slot(ptr, table)?.for_read());

    loop {
      let entry: DataEntry = slot.as_ref().deserialize()?;

      for record in entry.get_versions() {
        if self.0.is_visible(record.owner, record.version) {
          return Ok(true);
        }
      }

      match entry.get_next() {
        Some(i) => drop(replace(&mut slot, self.0.fetch_slot(i, table)?.for_read())),
        None => return Ok(false),
      }
    }
  }

  fn find_leaf_stack(
    &self,
    key: KeyRef,
    table: &Arc<TableHandle>,
  ) -> Result<(Pointer, Vec<Pointer>)> {
    let (mut ptr, height) = {
      let header = self
        .0
        .fetch_slot(HEADER_POINTER, table)?
        .for_read()
        .as_ref()
        .deserialize::<TreeHeader>()?;
      (header.get_root(), header.get_height())
    };
    let mut stack = vec![];

    while let BTreeNodeView::Internal(node) = self
      .0
      .fetch_slot(ptr, table)?
      .for_read()
      .as_ref()
      .view::<BTreeNodeView>()?
    {
      match node.find(&key) {
        Ok(i) => stack.push(replace(&mut ptr, i)),
        Err(i) => ptr = i,
      }
    }

    debug_assert_eq!(height, stack.len() as u16);
    Ok((ptr, stack))
  }

  pub fn scan(
    &self,
    table: &Arc<TableHandle>,
    start: &Bound<Key>,
    end: &Bound<Key>,
  ) -> Result<BTreeIterator<'_, Policy>> {
    BTreeIterator::open(&self.0, table, start, end)
  }
}

impl<Policy: WritablePolicy> BTreeIndex<Policy> {
  pub fn initialize(&self, table: &Arc<TableHandle>) -> Result {
    let root = self.0.alloc_and_log(&BTreeNode::initial_state(), table)?;

    {
      let header = TreeHeader::new(root);
      let mut slot = self.0.fetch_slot(HEADER_POINTER, table)?.for_write();
      self.0.serialize_and_log(&mut slot, &header, table)?;
    }

    Ok(())
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
      let slot = self.0.fetch_slot(ptr, table)?.for_write();
      let mut internal = slot.as_ref().deserialize::<BTreeNode>()?.as_internal()?;
      match internal.insert_or_next(&evicted_key, evicted_ptr) {
        Ok(_) => break (slot, internal),
        Err(i) => ptr = i,
      }
    };

    let (split_node, split_key) = match internal.split_if_needed() {
      Some(split) => split,
      None => {
        return self
          .0
          .serialize_and_log(&mut slot, &internal.to_node(), table)
          .map(|_| None);
      }
    };

    let split_index = self.0.alloc_and_log(&split_node.to_node(), table)?;

    internal.set_right(&split_key, split_index);
    self
      .0
      .serialize_and_log(&mut slot, &internal.to_node(), table)?;

    Ok(Some((split_key, split_index)))
  }

  fn create_entry<F>(
    &self,
    key: KeyRef,
    pos: usize,
    slot: &mut WritableSlot,
    mut node: LeafNode,
    table: &Arc<TableHandle>,
    create_record: F,
  ) -> Result<Option<(Key, Pointer)>>
  where
    F: FnOnce() -> VersionRecord,
  {
    let entry = DataEntry::init(create_record());
    let entry_ptr = self.0.alloc_and_log(&entry, table)?;

    let split = match node.insert_and_split(pos, key.to_vec(), entry_ptr) {
      Some(split) => split,
      None => {
        return self
          .0
          .serialize_and_log(slot, &node.to_node(), table)
          .map(|_| None);
      }
    };

    let mid_key = split.top().clone();
    let split_ptr = self.0.alloc_and_log(&split.to_node(), table)?;

    node.set_next(split_ptr);
    self.0.serialize_and_log(slot, &node.to_node(), table)?;

    Ok(Some((mid_key, split_ptr)))
  }

  fn propagate_split(
    &self,
    mut split_key: Key,
    mut split_pointer: Pointer,
    mut stack: Vec<Pointer>,
    table: &Arc<TableHandle>,
  ) -> Result {
    // CAS loop: multiple concurrent splits may race to update the root.
    loop {
      let old_height = stack.len() as u16;
      while let Some(ptr) = stack.pop() {
        match self.apply_split(split_key, split_pointer, ptr, table)? {
          Some((k, p)) => {
            split_key = k;
            split_pointer = p;
          }
          None => return Ok(()),
        };
      }

      let mut header_slot = self.0.fetch_slot(HEADER_POINTER, table)?.for_write();
      let mut header: TreeHeader = header_slot.as_ref().deserialize()?;
      let current_height = header.get_height();
      let mut ptr = header.get_root();
      if old_height == current_height {
        let new_root = InternalNode::initialize(split_key, ptr, split_pointer);
        let new_root_index = self.0.alloc_and_log(&new_root.to_node(), table)?;

        header.set_root(new_root_index);
        header.increase_height();
        return self.0.serialize_and_log(&mut header_slot, &header, table);
      }

      let diff = (current_height - old_height) as usize;
      drop(header_slot);

      while stack.len() < diff {
        let slot = self.0.fetch_slot(ptr, table)?.for_read();
        let node = slot.as_ref().view::<BTreeNodeView>()?.as_internal()?;
        match node.find(&split_key) {
          Ok(i) => stack.push(replace(&mut ptr, i)),
          Err(i) => ptr = i,
        }
      }
    }
  }

  pub fn create_record(
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
      pointers.push(self.0.alloc_and_log(&chunk, table)?);
      data = remain;
    }
    pointers.push(self.0.alloc_and_log(&DataChunk::new(data), table)?);

    Ok(RecordData::Chunked(pointers))
  }

  fn apply_version_chain<T>(
    &self,
    entry_ptr: Pointer,
    record: VersionRecord,
    coupling: T,
    table: &Arc<TableHandle>,
  ) -> Result {
    let mut slot = self.0.fetch_slot(entry_ptr, table)?.for_write();
    drop(coupling);

    let mut entry: DataEntry = slot.as_ref().deserialize()?;

    let mut new_versions = entry.take_versions().chain([record]).collect::<Vec<_>>();
    new_versions.sort_by(|a, b| b.version.cmp(&a.version));

    loop {
      while let Some(r) = new_versions.pop_if(|r| entry.is_available(r)) {
        entry.append(r);
      }

      if new_versions.is_empty() {
        self.0.serialize_and_log(&mut slot, &entry, table)?;
        break;
      }

      let new_ptr = self.0.alloc_and_log(&entry, table)?;
      entry = DataEntry::empty();
      entry.set_next(new_ptr);
    }

    self.0.when_update_entry(entry_ptr, table);
    Ok(())
  }

  pub fn apply_snapshot(&self, snapshot: KVSnapshot, table: &Arc<TableHandle>) -> Result {
    let key = snapshot.key;
    let record = VersionRecord::new(
      snapshot.owner,
      snapshot.version,
      self.create_record(snapshot.value, table)?,
    );
    let (mut ptr, stack) = self.find_leaf_stack(&key, table)?;

    let (mid_key, right_ptr) = loop {
      let mut slot = self.0.fetch_slot(ptr, table)?.for_write();

      let node = slot.as_ref().view::<BTreeNodeView>()?.as_leaf()?;
      match node.find(&key) {
        NodeFindResult::Found(_, i) => {
          return self.apply_version_chain(i, record, slot, table);
        }
        NodeFindResult::Move(i) => ptr = i,
        NodeFindResult::NotFound(i) => {
          let node = node.writable();
          match self.create_entry(&key, i, &mut slot, node, table, || record)? {
            Some(v) => break v,
            None => return Ok(()),
          };
        }
      }
    };
    self.propagate_split(mid_key, right_ptr, stack, table)?;
    Ok(())
  }
}
impl<Policy> BTreeIndex<Policy>
where
  Policy: CreatablePolicy,
{
  pub fn insert_record(
    &self,
    key: Key,
    record: RecordData,
    table: &Arc<TableHandle>,
  ) -> Result {
    let (mut ptr, stack) = self.find_leaf_stack(&key, table)?;

    let (mid_key, right_ptr) = loop {
      let mut slot = self.0.fetch_slot(ptr, table)?.for_write();

      let node = slot.as_ref().view::<BTreeNodeView>()?.as_leaf()?;
      match node.find(&key) {
        NodeFindResult::Found(_, i) => return self.insert_at(i, record, slot, table),
        NodeFindResult::Move(i) => ptr = i,
        NodeFindResult::NotFound(i) => {
          let node = node.writable();
          match self.create_entry(&key, i, &mut slot, node, table, || {
            VersionRecord::new(self.0.current_owner(), self.0.current_version(), record)
          })? {
            Some(v) => break v,
            None => return Ok(()),
          };
        }
      }
    };
    self.propagate_split(mid_key, right_ptr, stack, table)?;
    Ok(())
  }
  pub fn insert(&self, key: Key, data: Vec<u8>, table: &Arc<TableHandle>) -> Result {
    self.insert_record(key, self.create_record(data, table)?, table)
  }
  pub fn remove(&self, key: KeyRef, table: &Arc<TableHandle>) -> Result {
    self.insert_record_if_matched(key, RecordData::Tombstone, table)
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
    let mut slot = self.0.fetch_slot(entry_ptr, table)?.for_write();
    drop(coupling);

    let mut entry: DataEntry = slot.as_ref().deserialize()?;
    if let Some(owner) = entry.get_last_owner() {
      if self.0.is_conflict(owner) {
        return Err(Error::WriteConflict);
      }
    }

    let record =
      VersionRecord::new(self.0.current_owner(), self.0.current_version(), data);

    if entry.is_available(&record) {
      entry.append(record);
      self.0.when_update_entry(entry_ptr, table);
      return self.0.serialize_and_log(&mut slot, &entry, table);
    }

    let new_entry_index = self.0.alloc_and_log(&entry, table)?;

    let mut new_entry = DataEntry::init(record);
    new_entry.set_next(new_entry_index);
    self.0.serialize_and_log(&mut slot, &new_entry, table)?;

    self.0.when_update_entry(entry_ptr, table);
    Ok(())
  }

  pub fn insert_if_matched(
    &self,
    key: KeyRef,
    data: Vec<u8>,
    table: &Arc<TableHandle>,
  ) -> Result {
    self.insert_record_if_matched(key, self.create_record(data, table)?, table)
  }

  pub fn insert_record_if_matched(
    &self,
    key: KeyRef,
    record: RecordData,
    table: &Arc<TableHandle>,
  ) -> Result {
    match self.get_entry(key, table)? {
      Some((slot, ptr)) => self.insert_at(ptr, record, slot, table),
      None => Ok(()),
    }
  }
}

pub struct KVSnapshot {
  key: Key,
  value: Vec<u8>,
  owner: TxId,
  version: TxId,
}

pub struct BTreeIterator<'a, Policy> {
  policy: &'a Policy,
  table: Arc<TableHandle>,
  keys: VecDeque<(Key, Pointer)>,
  next: Option<Pointer>,
  end: Bound<Key>,
  closed: bool,
}
impl<'a, Policy> BTreeIterator<'a, Policy>
where
  Policy: ReadonlyPolicy,
{
  pub fn open(
    policy: &'a Policy,
    table: &Arc<TableHandle>,
    start: &Bound<Key>,
    end: &Bound<Key>,
  ) -> Result<Self> {
    let mut ptr = policy
      .fetch_slot(HEADER_POINTER, table)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    loop {
      let mut slot = policy.fetch_slot(ptr, table)?.for_read();
      match slot.as_ref().view::<BTreeNodeView>()? {
        BTreeNodeView::Internal(node) => match &start {
          Bound::Included(k) => ptr = node.find(k).unwrap_or_else(|i| i),
          Bound::Excluded(k) => ptr = node.find(k).unwrap_or_else(|i| i),
          Bound::Unbounded => ptr = node.first_child(),
        },
        BTreeNodeView::Leaf(mut node) => {
          let pos = match &start {
            Bound::Unbounded => 0,
            Bound::Included(k) => loop {
              match node.find(k) {
                NodeFindResult::Found(i, _) => break i,
                NodeFindResult::NotFound(i) => break i,
                NodeFindResult::Move(i) => {
                  let _ = replace(&mut slot, policy.fetch_slot(i, table)?.for_read());
                  node = slot.as_ref().view::<BTreeNodeView>()?.as_leaf()?;
                }
              }
            },
            Bound::Excluded(k) => loop {
              match node.find(k) {
                NodeFindResult::Found(i, _) => break i + 1,
                NodeFindResult::NotFound(i) => break i,
                NodeFindResult::Move(i) => {
                  let _ = replace(&mut slot, policy.fetch_slot(i, table)?.for_read());
                  node = slot.as_ref().view::<BTreeNodeView>()?.as_leaf()?;
                }
              }
            },
          };

          let keys: VecDeque<_> = match &end {
            Bound::Included(e) => node
              .get_entries()
              .skip(pos)
              .take_while(|(k, _)| *k <= e)
              .map(|(k, p)| (k.to_vec(), p))
              .collect(),
            Bound::Excluded(e) => node
              .get_entries()
              .skip(pos)
              .take_while(|(k, _)| *k < e)
              .map(|(k, p)| (k.to_vec(), p))
              .collect(),
            Bound::Unbounded => node
              .get_entries()
              .skip(pos)
              .map(|(k, p)| (k.to_vec(), p))
              .collect(),
          };

          let mut next = None;
          if node.len() - pos == keys.len() {
            next = node.get_next();
          }

          return Ok(Self {
            policy,
            table: table.clone(),
            keys,
            next,
            end: end.clone(),
            closed: false,
          });
        }
      }
    }
  }

  fn find_value(&self, ptr: Pointer) -> Result<Option<Option<(Vec<u8>, TxId, TxId)>>> {
    let mut slot = self.policy.fetch_slot(ptr, &self.table)?.for_read();
    loop {
      let entry = slot.as_ref().deserialize::<DataEntry>()?;

      for record in entry.get_versions() {
        if self.policy.is_visible(record.owner, record.version) {
          return match record
            .data
            .read_data(|i| self.policy.fetch_and_deserialize(i, &self.table))?
          {
            Some(v) => Ok(Some(Some((v, record.owner, record.version)))),
            None => Ok(Some(None)),
          };
        }
      }

      match entry.get_next() {
        Some(i) => drop(replace(
          &mut slot,
          self.policy.fetch_slot(i, &self.table)?.for_read(),
        )),
        None => return Ok(None),
      }
    }
  }

  fn fill_up(&mut self) -> Result {
    let ptr = match self.next.take() {
      Some(p) => p,
      None => {
        self.closed = true;
        return Ok(());
      }
    };

    let slot = self.policy.fetch_slot(ptr, &self.table)?.for_read();
    let node = slot.as_ref().view::<BTreeNodeView>()?.as_leaf()?;

    match &self.end {
      Bound::Included(end) => node
        .get_entries()
        .take_while(|(k, _)| *k <= end)
        .for_each(|(k, p)| self.keys.push_back((k.to_vec(), p))),
      Bound::Excluded(end) => node
        .get_entries()
        .take_while(|(k, _)| *k < end)
        .for_each(|(k, p)| self.keys.push_back((k.to_vec(), p))),
      Bound::Unbounded => node
        .get_entries()
        .for_each(|(k, p)| self.keys.push_back((k.to_vec(), p))),
    }

    if node.len() == self.keys.len() {
      self.next = node.get_next();
    }
    Ok(())
  }

  fn next_record(&mut self) -> Result<Option<(Key, Option<(Vec<u8>, TxId, TxId)>)>> {
    loop {
      if self.closed {
        return Ok(None);
      }

      while let Some((key, ptr)) = self.keys.pop_front() {
        if let Some(value) = self.find_value(ptr)? {
          return Ok(Some((key, value)));
        }
      }

      self.fill_up()?;
    }
  }

  pub fn snapshot(&mut self) -> Result<Option<KVSnapshot>> {
    loop {
      match self.next_record()? {
        Some((_, None)) => continue,
        None => return Ok(None),
        Some((key, Some((value, owner, version)))) => {
          return Ok(Some(KVSnapshot {
            key,
            value,
            owner,
            version,
          }));
        }
      }
    }
  }

  pub fn next_kv_skip_tombstone(&mut self) -> Result<Option<(Key, Vec<u8>)>> {
    loop {
      match self.next_record()? {
        Some((_, None)) => continue,
        None => return Ok(None),
        Some((key, Some((value, _, _)))) => return Ok(Some((key, value))),
      }
    }
  }

  pub fn next_kv(&mut self) -> Result<Option<(Key, Option<Vec<u8>>)>> {
    match self.next_record()? {
      Some((k, v)) => Ok(Some((k, v.map(|v| v.0)))),
      None => Ok(None),
    }
  }
}
