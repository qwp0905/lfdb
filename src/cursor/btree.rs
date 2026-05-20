use std::{collections::VecDeque, mem::replace, ops::Bound, sync::Arc};

use crate::{
  cache::RefedSlot, disk::Pointer, table::TableHandle, wal::TxId, Error, Result,
};

use crossbeam::epoch::pin;

use super::{
  BTreeNode, BTreeNodeView, CreatablePolicy, DataChunk, DataChunkView, DataEntry,
  DataEntryView, InternalNode, LeafNode, NodeFindResult, ReadonlyPolicy, RecordData,
  RecordDataView, StaticKey, StaticKeyRef, TreeHeader, VecRef, VersionRecord,
  WritablePolicy, CHUNK_SIZE, HEADER_POINTER, LARGE_VALUE,
};

pub struct BTreeIndex<Policy>(Policy);
impl<Policy> BTreeIndex<Policy> {
  pub const fn new(policy: Policy) -> Self {
    Self(policy)
  }
}
impl<Policy: ReadonlyPolicy> BTreeIndex<Policy> {
  fn get_entry(
    &self,
    key: StaticKeyRef,
    table: &Arc<TableHandle>,
  ) -> Result<Option<Pointer>> {
    let mut ptr = self
      .0
      .fetch_slot(HEADER_POINTER, table)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    loop {
      // This guard protects the next node or data entry from the GC or tree manager.
      // By declaring a guard before reading the current page, it is guaranteed that the pointers written to the current page have been reclaimed and are not reused.
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      match slot.as_ref().view::<BTreeNodeView>()? {
        BTreeNodeView::Internal(node) => ptr = node.find(key).unwrap_or_else(|i| i),
        BTreeNodeView::Leaf(node) => match node.find(key) {
          NodeFindResult::Found(_, i) => return Ok(Some(i)),
          NodeFindResult::NotFound(_) => return Ok(None),
          NodeFindResult::Move(i) => ptr = i,
        },
      }
    }
  }

  fn read_chunk(
    policy: &Policy,
    pointers: &[Pointer],
    table: &Arc<TableHandle>,
  ) -> Result<VecRef> {
    let mut data = Vec::new();

    for &ptr in pointers {
      let slot = policy.fetch_slot(ptr, table)?.for_read();
      let chunk: DataChunkView = slot.as_ref().view()?;
      data.extend_from_slice(chunk.get_data());
    }

    Ok(VecRef::copied(data))
  }

  pub fn get(
    &self,
    key: StaticKeyRef,
    table: &Arc<TableHandle>,
  ) -> Result<Option<Option<VecRef>>> {
    let ptr = match self.get_entry(key, table)? {
      Some(v) => v,
      None => return Ok(None),
    };

    let mut _guard = None;
    let mut next = Some(ptr);
    while let Some(ptr) = next.take() {
      let new_guard = pin();
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      let entry: DataEntryView = slot.as_ref().deserialize()?;

      if let Some(record) =
        entry.find(|&record| self.0.is_visible(record.owner, record.version))
      {
        return Ok(Some(match &record.data {
          RecordDataView::Data(s, e) => Some(VecRef::refed(slot.page(), *s, *e)),
          RecordDataView::Chunked(pointers) => {
            Some(Self::read_chunk(&self.0, pointers, table)?)
          }
          RecordDataView::Tombstone => None,
        }));
      }

      next = entry.get_next();
      _guard = Some(new_guard);
    }

    Ok(None)
  }

  pub fn contains(&self, key: StaticKeyRef, table: &Arc<TableHandle>) -> Result<bool> {
    let ptr = match self.get_entry(key, table)? {
      Some(v) => v,
      None => return Ok(false),
    };

    let mut _guard = None;
    let mut next = Some(ptr);
    while let Some(ptr) = next.take() {
      let new_guard = pin();
      let entry: DataEntryView = self
        .0
        .fetch_slot(ptr, table)?
        .for_read()
        .as_ref()
        .deserialize()?;

      if let Some(record) =
        entry.find(|&record| self.0.is_visible(record.owner, record.version))
      {
        return Ok(match &record.data {
          RecordDataView::Chunked(_) | RecordDataView::Data(_, _) => true,
          RecordDataView::Tombstone => false,
        });
      };

      next = entry.get_next();
      _guard = Some(new_guard);
    }

    Ok(false)
  }

  fn find_leaf_stack(
    &self,
    key: StaticKeyRef,
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
    start: &Bound<StaticKey>,
    end: &Bound<StaticKey>,
  ) -> Result<BTreeIterator<'_, Policy>> {
    BTreeIterator::open(&self.0, table, start, end)
  }
}

impl<Policy: WritablePolicy> BTreeIndex<Policy> {
  pub fn initialize(&self, table: &Arc<TableHandle>) -> Result {
    let root = self.0.alloc_and_log(&BTreeNode::initial_state(), table)?;
    {
      let mut slot = self.0.alloc_slot(HEADER_POINTER, table)?.for_write();
      self
        .0
        .serialize_and_log(&mut slot, &TreeHeader::new(root), table)?;
    }

    Ok(())
  }

  fn apply_split(
    &self,
    evicted_key: StaticKey,
    evicted_ptr: Pointer,
    current: Pointer,
    table: &Arc<TableHandle>,
  ) -> Result<Option<(StaticKey, Pointer)>> {
    let mut ptr = current;

    let mut result = None;
    let mut stop = false;
    while !stop {
      self.0.fetch_slot(ptr, table)?.for_batch().mutate(|slot| {
        let mut internal = slot.as_ref().deserialize::<BTreeNode>()?.as_internal()?;
        if let Err(i) = internal.insert_or_next(&evicted_key, evicted_ptr) {
          return Ok(ptr = i);
        };

        stop = true;
        let (split_node, split_key) = match internal.split_if_needed() {
          Some(v) => v,
          None => {
            self.0.serialize_and_log(slot, &internal.to_node(), table)?;
            return Ok(());
          }
        };

        let split_index = self.0.alloc_and_log(&split_node.to_node(), table)?;
        internal.set_right(&split_key, split_index);
        self.0.serialize_and_log(slot, &internal.to_node(), table)?;
        result = Some((split_key, split_index));

        Ok(())
      })?;
    }
    Ok(result)
  }

  fn create_entry<F>(
    &self,
    key: StaticKeyRef,
    pos: usize,
    slot: &mut RefedSlot,
    mut node: LeafNode,
    table: &Arc<TableHandle>,
    create_record: F,
  ) -> Result<Option<(StaticKey, Pointer)>>
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
    mut split_key: StaticKey,
    mut split_pointer: Pointer,
    mut stack: Vec<Pointer>,
    table: &Arc<TableHandle>,
  ) -> Result {
    // CAS loop: multiple concurrent splits may race to update the root.
    loop {
      let old_height = stack.len() as u16;
      while let Some(ptr) = stack.pop() {
        match self.apply_split(split_key.clone(), split_pointer, ptr, table)? {
          Some((k, p)) => {
            split_key = k;
            split_pointer = p;
          }
          None => return Ok(()),
        };
      }

      let mut changed = None;
      self
        .0
        .fetch_slot(HEADER_POINTER, table)?
        .for_batch()
        .mutate(|header_slot| {
          let mut header: TreeHeader = header_slot.as_ref().deserialize()?;
          let current_height = header.get_height();
          let ptr = header.get_root();
          if old_height == current_height {
            let new_root =
              InternalNode::initialize(split_key.clone(), ptr, split_pointer);
            let new_root_index = self.0.alloc_and_log(&new_root.to_node(), table)?;

            header.set_root(new_root_index);
            header.increase_height();
            return self.0.serialize_and_log(header_slot, &header, table);
          }

          changed = Some((ptr, (current_height - old_height) as usize));

          Ok(())
        })?;

      let (mut ptr, diff) = match changed {
        Some(v) => v,
        None => return Ok(()),
      };

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
      pointers.push(self.0.alloc_and_log(&chunk, table)?);
      data = remain;
    }
    pointers.push(self.0.alloc_and_log(&DataChunk::new(data), table)?);

    Ok(RecordData::Chunked(pointers))
  }

  fn apply_version_chain(
    &self,
    entry_ptr: Pointer,
    record: VersionRecord,
    table: &Arc<TableHandle>,
  ) -> Result {
    let mut slot = self.0.fetch_slot(entry_ptr, table)?.for_write();
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
      self.create_record(snapshot.value.into_vec(), table)?,
    );
    let (mut ptr, stack) = self.find_leaf_stack(&key, table)?;

    let mut record = Some(record);
    loop {
      let mut result = None;
      self.0.fetch_slot(ptr, table)?.for_batch().mutate(|slot| {
        let node = slot.as_ref().view::<BTreeNodeView>()?.as_leaf()?;
        match node.find(&key) {
          NodeFindResult::Found(_, i) => result = Some(Ok(i)),
          NodeFindResult::Move(i) => ptr = i,
          NodeFindResult::NotFound(i) => {
            let node = node.writable();
            let split =
              self.create_entry(&key, i, slot, node, table, || record.take().unwrap())?;
            result = Some(Err(split))
          }
        }
        Ok(())
      })?;

      match result {
        Some(Ok(i)) => return self.apply_version_chain(i, record.take().unwrap(), table),
        Some(Err(Some((k, p)))) => return self.propagate_split(k, p, stack, table),
        Some(Err(None)) => return Ok(()),
        None => continue,
      }
    }
  }
}
impl<Policy> BTreeIndex<Policy>
where
  Policy: CreatablePolicy,
{
  pub fn insert_record(
    &self,
    key: StaticKey,
    record: RecordData,
    table: &Arc<TableHandle>,
  ) -> Result {
    let (mut ptr, stack) = self.find_leaf_stack(&key, table)?;

    let mut record = Some(record);
    loop {
      let mut result = None;
      self.0.fetch_slot(ptr, table)?.for_batch().mutate(|slot| {
        let node = slot.as_ref().view::<BTreeNodeView>()?.as_leaf()?;
        match node.find(&key) {
          NodeFindResult::Found(_, i) => result = Some(Ok(i)),
          NodeFindResult::Move(i) => ptr = i,
          NodeFindResult::NotFound(i) => {
            let node = node.writable();
            let split = self.create_entry(&key, i, slot, node, table, || {
              VersionRecord::new(
                self.0.current_owner(),
                self.0.current_version(),
                record.take().unwrap(),
              )
            })?;
            result = Some(Err(split))
          }
        }
        Ok(())
      })?;

      match result {
        Some(Ok(i)) => return self.insert_at(i, record.take().unwrap(), table),
        Some(Err(Some((k, p)))) => return self.propagate_split(k, p, stack, table),
        Some(Err(None)) => return Ok(()),
        None => continue,
      }
    }
  }
  pub fn insert(
    &self,
    key: StaticKey,
    data: Vec<u8>,
    table: &Arc<TableHandle>,
  ) -> Result {
    self.insert_record(key, self.create_record(data, table)?, table)
  }
  pub fn remove(&self, key: StaticKeyRef, table: &Arc<TableHandle>) -> Result {
    self.insert_record_if_matched(key, RecordData::Tombstone, table)
  }

  /**
   * coupling required because of gc can collect entry header before write lock.
   */
  fn insert_at(
    &self,
    entry_ptr: Pointer,
    data: RecordData,
    table: &Arc<TableHandle>,
  ) -> Result {
    let mut slot = self.0.fetch_slot(entry_ptr, table)?.for_write();
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
      self.0.serialize_and_log(&mut slot, &entry, table)?;
      self.0.when_update_entry(entry_ptr, table);
      return Ok(());
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
    key: StaticKeyRef,
    data: Vec<u8>,
    table: &Arc<TableHandle>,
  ) -> Result {
    self.insert_record_if_matched(key, self.create_record(data, table)?, table)
  }

  fn insert_record_if_matched(
    &self,
    key: StaticKeyRef,
    record: RecordData,
    table: &Arc<TableHandle>,
  ) -> Result {
    match self.get_entry(&key, table)? {
      Some(ptr) => self.insert_at(ptr, record, table),
      None => return Ok(()),
    }
  }
}

enum Buffered {
  Data(VecRef),
  Chunked(Vec<Pointer>),
}

pub struct KVSnapshot {
  key: VecRef,
  value: VecRef,
  owner: TxId,
  version: TxId,
}

pub struct BTreeIterator<'a, Policy> {
  policy: &'a Policy,
  table: Arc<TableHandle>,
  buffered: VecDeque<(VecRef, Pointer)>,
  next: Option<Pointer>,
  end: Bound<StaticKey>,
  closed: bool,
}
impl<'a, Policy> BTreeIterator<'a, Policy>
where
  Policy: ReadonlyPolicy,
{
  pub fn open(
    policy: &'a Policy,
    table: &Arc<TableHandle>,
    start: &Bound<StaticKey>,
    end: &Bound<StaticKey>,
  ) -> Result<Self> {
    let mut ptr = policy
      .fetch_slot(HEADER_POINTER, table)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    let mut buffered = VecDeque::new();

    loop {
      let slot = policy.fetch_slot(ptr, table)?.for_read();
      match slot.as_ref().view::<BTreeNodeView>()? {
        BTreeNodeView::Internal(node) => match &start {
          Bound::Included(k) => ptr = node.find(k).unwrap_or_else(|i| i),
          Bound::Excluded(k) => ptr = node.find(k).unwrap_or_else(|i| i),
          Bound::Unbounded => ptr = node.first_child(),
        },
        BTreeNodeView::Leaf(node) => {
          let pos = match &start {
            Bound::Included(k) => match node.find(k) {
              NodeFindResult::Found(i, _) => i,
              NodeFindResult::NotFound(i) => i,
              NodeFindResult::Move(i) => {
                ptr = i;
                continue;
              }
            },
            Bound::Excluded(k) => match node.find(k) {
              NodeFindResult::Found(i, _) => i + 1,
              NodeFindResult::NotFound(i) => i,
              NodeFindResult::Move(i) => {
                ptr = i;
                continue;
              }
            },
            Bound::Unbounded => 0,
          };

          for (s, e, p) in node.get_entries_while(end).skip(pos) {
            buffered.push_back((VecRef::refed(slot.page(), s, e), p));
          }

          let mut next = None;
          if buffered.len() == node.len() - pos {
            next = node.get_next();
          }

          return Ok(Self {
            policy,
            table: table.clone(),
            buffered,
            next,
            end: end.clone(),
            closed: false,
          });
        }
      }
    }
  }

  fn find_value(&self, ptr: Pointer) -> Result<Option<Option<(Buffered, TxId, TxId)>>> {
    let mut next = Some(ptr);

    let mut _guard = None;
    while let Some(ptr) = next.take() {
      let new_guard = pin();
      let slot = self.policy.fetch_slot(ptr, &self.table)?.for_read();
      let entry: DataEntryView = slot.as_ref().deserialize()?;

      if let Some(record) =
        entry.find(|record| self.policy.is_visible(record.owner, record.version))
      {
        return Ok(Some(match &record.data {
          RecordDataView::Data(s, e) => Some((
            Buffered::Data(VecRef::refed(slot.page(), *s, *e)),
            record.owner,
            record.version,
          )),
          RecordDataView::Chunked(pointers) => Some((
            Buffered::Chunked(pointers.to_vec()),
            record.owner,
            record.version,
          )),
          RecordDataView::Tombstone => None,
        }));
      }

      next = entry.get_next();
      _guard = Some(new_guard);
    }

    Ok(None)
  }

  fn fill_up(&mut self) -> Result {
    debug_assert!(self.buffered.is_empty());

    let ptr = match self.next.take() {
      Some(v) => v,
      None => {
        self.closed = true;
        return Ok(());
      }
    };

    let slot = self.policy.fetch_slot(ptr, &self.table)?.for_read();
    let node = slot.as_ref().view::<BTreeNodeView>()?.as_leaf()?;

    for (s, e, p) in node.get_entries_while(&self.end) {
      self
        .buffered
        .push_back((VecRef::refed(slot.page(), s, e), p));
    }

    if self.buffered.len() == node.len() {
      self.next = node.get_next();
    }
    Ok(())
  }

  fn next_record(&mut self) -> Result<Option<(VecRef, Option<(VecRef, TxId, TxId)>)>> {
    loop {
      if self.closed {
        return Ok(None);
      }

      if let Some((key, ptr)) = self.buffered.pop_front() {
        if let Some(found) = self.find_value(ptr)? {
          return Ok(Some(match found {
            Some((Buffered::Data(data), o, v)) => (key, Some((data, o, v))),
            Some((Buffered::Chunked(pointers), o, v)) => (
              key,
              Some((
                BTreeIndex::read_chunk(self.policy, &pointers, &self.table)?,
                o,
                v,
              )),
            ),
            None => (key, None),
          }));
        }
      }

      self.fill_up()?;
    }
  }

  pub fn next_snapshot(&mut self) -> Result<Option<KVSnapshot>> {
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

  pub fn next_kv_skip_tombstone(&mut self) -> Result<Option<(VecRef, VecRef)>> {
    loop {
      match self.next_record()? {
        Some((key, Some((value, _, _)))) => return Ok(Some((key, value))),
        Some((_, None)) => continue,
        None => return Ok(None),
      }
    }
  }

  pub fn next_kv(&mut self) -> Result<Option<(VecRef, Option<VecRef>)>> {
    match self.next_record()? {
      Some((k, v)) => Ok(Some((k, v.map(|v| v.0)))),
      None => Ok(None),
    }
  }
}
