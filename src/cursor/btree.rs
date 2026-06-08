use std::{collections::VecDeque, mem::replace, ops::Bound};

use crate::{
  cache::ReadonlySlot, disk::Pointer, table::TableHandleRef, wal::TxId, Error, Result,
};

use crossbeam::epoch::pin;

use super::{
  BTreeNode, BTreeNodeView, CreatablePolicy, DataChunk, DataChunkView, DataEntry,
  DataEntryView, InternalNode, MergeSortable, NodeFindResult, ReadonlyPolicy, RecordData,
  RecordDataView, StaticKey, StaticKeyRef, TreeHeader, VecRef, VersionRecord,
  VersionRecordView, WritablePolicy, CHUNK_SIZE, HEADER_POINTER, LARGE_VALUE,
};

pub struct BTreeIndex<Policy>(Policy);
impl<Policy> BTreeIndex<Policy> {
  pub const fn new(policy: Policy) -> Self {
    Self(policy)
  }
}
impl<Policy: ReadonlyPolicy> BTreeIndex<Policy> {
  fn read_chunk(
    policy: &Policy,
    pointers: &[Pointer],
    table: &TableHandleRef,
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
    table: &TableHandleRef,
  ) -> Result<Option<Option<VecRef>>> {
    let mut ptr = self
      .0
      .fetch_slot(HEADER_POINTER, table)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    loop {
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      match slot.as_ref().view::<BTreeNodeView>()? {
        BTreeNodeView::Internal(node) => ptr = node.find(key)?.unwrap_or_else(|i| i),
        BTreeNodeView::Leaf(node) => match node.find(key)? {
          NodeFindResult::NotFound(_) => return Ok(None),
          NodeFindResult::Move(i) => ptr = i,
          NodeFindResult::Found(_, record, i) => {
            if !self.0.is_visible(record.owner, record.version) {
              break ptr = i;
            }
            return Ok(Some(match &record.data {
              RecordDataView::Data(s, e) => Some(VecRef::refed(slot.page(), *s, *e)),
              RecordDataView::Chunked(pointers) => {
                Some(Self::read_chunk(&self.0, pointers, table)?)
              }
              RecordDataView::Tombstone => None,
            }));
          }
        },
      }
    }

    let mut _guard = None;
    let mut next = Some(ptr);
    while let Some(ptr) = next.take() {
      let new_guard = pin();
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      let entry: DataEntryView = slot.as_ref().view()?;

      if let Some(record) =
        entry.find(|record| self.0.is_visible(record.owner, record.version))?
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

  pub fn contains(&self, key: StaticKeyRef, table: &TableHandleRef) -> Result<bool> {
    let mut ptr = self
      .0
      .fetch_slot(HEADER_POINTER, table)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    loop {
      // This guard protects the next node or data entry from the GC.
      // By declaring a guard before reading the current page, it is guaranteed that the pointers written to the current page have been reclaimed and are not reused.
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      match slot.as_ref().view::<BTreeNodeView>()? {
        BTreeNodeView::Internal(node) => ptr = node.find(key)?.unwrap_or_else(|i| i),
        BTreeNodeView::Leaf(node) => match node.find(key)? {
          NodeFindResult::NotFound(_) => return Ok(false),
          NodeFindResult::Move(i) => ptr = i,
          NodeFindResult::Found(_, record, i) => {
            if !self.0.is_visible(record.owner, record.version) {
              break ptr = i;
            }
            return Ok(match &record.data {
              RecordDataView::Chunked(_) | RecordDataView::Data(_, _) => true,
              RecordDataView::Tombstone => false,
            });
          }
        },
      }
    }

    let mut _guard = None;
    let mut next = Some(ptr);
    while let Some(ptr) = next.take() {
      let new_guard = pin();
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      let entry: DataEntryView = slot.as_ref().view()?;
      if let Some(record) =
        entry.find(|record| self.0.is_visible(record.owner, record.version))?
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
    table: &TableHandleRef,
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
      match node.find(&key)? {
        Ok(i) => stack.push(replace(&mut ptr, i)),
        Err(i) => ptr = i,
      }
    }

    debug_assert_eq!(height, stack.len() as u16);
    Ok((ptr, stack))
  }

  pub fn scan(
    &self,
    table: &TableHandleRef,
    start: &Bound<StaticKey>,
    end: &Bound<StaticKey>,
  ) -> Result<BTreeIterator<&'_ Policy>> {
    BTreeIterator::open(&self.0, table, start, end)
  }
}

impl<Policy: ReadonlyPolicy + Clone> BTreeIndex<Policy> {
  pub fn snapshot(&self, table: &TableHandleRef) -> Result<Snapshotter<Policy>> {
    Snapshotter::open(self.0.clone(), table)
  }
}

impl<Policy: WritablePolicy> BTreeIndex<Policy> {
  pub fn initialize(&self, table: &TableHandleRef) -> Result {
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
    table: &TableHandleRef,
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

        let split_ptr = self.0.alloc_and_log(&split_node.to_node(), table)?;
        internal.set_right(&split_key, split_ptr);
        self.0.serialize_and_log(slot, &internal.to_node(), table)?;
        result = Some((split_key, split_ptr));

        Ok(())
      })?;
    }
    Ok(result)
  }

  fn propagate_split(
    &self,
    mut split_key: StaticKey,
    mut split_pointer: Pointer,
    mut stack: Vec<Pointer>,
    table: &TableHandleRef,
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
            let new_root_ptr = self.0.alloc_and_log(&new_root.to_node(), table)?;

            header.set_root(new_root_ptr);
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
        match node.find(&split_key)? {
          Ok(i) => stack.push(replace(&mut ptr, i)),
          Err(i) => ptr = i,
        }
      }
    }
  }

  fn create_record(
    &self,
    data: Option<Vec<u8>>,
    table: &TableHandleRef,
  ) -> Result<RecordData> {
    let mut data = match data {
      Some(v) => v,
      None => return Ok(RecordData::Tombstone),
    };
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
    table: &TableHandleRef,
  ) -> Result {
    self
      .0
      .fetch_slot(entry_ptr, table)?
      .for_batch()
      .mutate(|slot| {
        let mut entry: DataEntry = slot.as_ref().deserialize()?;

        let mut new_versions = entry.take_versions().chain([record]).collect::<Vec<_>>();
        new_versions.sort_by(|a, b| b.version.cmp(&a.version));

        loop {
          while let Some(r) = new_versions.pop_if(|r| entry.is_available(r)) {
            entry.append(r);
          }

          if new_versions.is_empty() {
            self.0.serialize_and_log(slot, &entry, table)?;
            break;
          }

          let new_ptr = self.0.alloc_and_log(&entry, table)?;
          entry = DataEntry::empty();
          entry.set_next(new_ptr);
        }

        Ok(())
      })
  }
  fn append_version_chain(
    &self,
    entry_ptr: Pointer,
    record: VersionRecord,
    table: &TableHandleRef,
  ) -> Result {
    self
      .0
      .fetch_slot(entry_ptr, table)?
      .for_batch()
      .mutate(|slot| {
        let mut entry: DataEntry = slot.as_ref().deserialize()?;

        if entry.is_available(&record) {
          entry.append(record);
          self.0.serialize_and_log(slot, &entry, table)?;
          return Ok(());
        }

        let new_entry_ptr = self.0.alloc_and_log(&entry, table)?;
        let new_entry = DataEntry::init(record, Some(new_entry_ptr));
        self.0.serialize_and_log(slot, &new_entry, table)?;

        Ok(())
      })?;

    Ok(())
  }

  pub fn apply_snapshot(&self, snapshot: KVSnapshot, table: &TableHandleRef) -> Result {
    let key = snapshot.key;
    let record = VersionRecord::new(
      snapshot.owner,
      snapshot.version,
      self.create_record(Some(snapshot.value.into_vec()), table)?,
    );
    let (mut ptr, stack) = self.find_leaf_stack(&key, table)?;

    let mut record = Some(record);
    loop {
      let mut result = None;
      self.0.fetch_slot(ptr, table)?.for_batch().mutate(|slot| {
        let leaf = slot.as_ref().view::<BTreeNodeView>()?.as_leaf()?;
        let mut node = match leaf.find(&key)? {
          NodeFindResult::Move(i) => return Ok(ptr = i),
          NodeFindResult::Found(i, old, entry_ptr) => {
            let is_aborted = self.0.is_aborted(old.owner);
            if old.version > snapshot.version && !is_aborted {
              return Ok(result = Some(Err(entry_ptr)));
            }

            let mut node = leaf.writable()?;
            let new_record = record.take().unwrap();
            let old = node.replace_at(i, new_record);
            if !is_aborted {
              self.append_version_chain(entry_ptr, old, table)?;
            } else if let RecordData::Chunked(pointers) = old.data {
              pointers.into_iter().for_each(|p| table.free().dealloc(p));
            }
            node
          }
          NodeFindResult::NotFound(i) => {
            let mut node = leaf.writable()?;
            let entry_ptr = self.0.alloc_and_log(&DataEntry::empty(), table)?;
            let new_record = record.take().unwrap();
            node.insert_at(i, key.to_vec(), new_record, entry_ptr);
            node
          }
        };

        let split = match node.split_if_needed() {
          Some(split) => split,
          None => {
            self.0.serialize_and_log(slot, &node.to_node(), table)?;
            return Ok(result = Some(Ok(None)));
          }
        };

        let mid_key = split.top().clone();
        let split_ptr = self.0.alloc_and_log(&split.to_node(), table)?;

        node.set_next(split_ptr);
        self.0.serialize_and_log(slot, &node.to_node(), table)?;
        Ok(result = Some(Ok(Some((mid_key, split_ptr)))))
      })?;

      match result {
        Some(Ok(Some((k, p)))) => return self.propagate_split(k, p, stack, table),
        Some(Ok(None)) => return Ok(()),
        Some(Err(i)) => {
          return self.apply_version_chain(i, record.take().unwrap(), table)
        }
        None => continue,
      }
    }
  }
}
impl<Policy> BTreeIndex<Policy>
where
  Policy: CreatablePolicy,
{
  fn __insert(
    &self,
    key: StaticKeyRef,
    mut record: Option<Vec<u8>>,
    table: &TableHandleRef,
    create: bool,
  ) -> Result {
    let (mut ptr, stack) = self.find_leaf_stack(&key, table)?;

    loop {
      let mut state = LoopState::Continue;
      self.0.fetch_slot(ptr, table)?.for_batch().mutate(|slot| {
        let leaf = slot.as_ref().view::<BTreeNodeView>()?.as_leaf()?;
        let mut node = match leaf.find(&key)? {
          NodeFindResult::Move(i) => return Ok(ptr = i),
          NodeFindResult::Found(i, old, entry_ptr) => {
            if self.0.is_conflict(old.owner, old.version) {
              return Ok(state = LoopState::Conflict(old.owner));
            }

            let mut node = leaf.writable()?;
            let new_record = VersionRecord::new(
              self.0.current_owner(),
              self.0.current_version(),
              self.create_record(record.take(), table)?,
            );
            let old = node.replace_at(i, new_record);
            if !self.0.is_aborted(old.owner) && !self.0.is_owned(old.owner) {
              self.append_version_chain(entry_ptr, old, table)?;
            } else if let RecordData::Chunked(pointers) = old.data {
              pointers.into_iter().for_each(|p| table.free().dealloc(p));
            }
            node
          }
          NodeFindResult::NotFound(i) => {
            if !create {
              return Ok(state = LoopState::Break);
            }

            let mut node = leaf.writable()?;
            let entry_ptr = self.0.alloc_and_log(&DataEntry::empty(), table)?;

            let new_record = VersionRecord::new(
              self.0.current_owner(),
              self.0.current_version(),
              self.create_record(record.take(), &table)?,
            );
            node.insert_at(i, key.to_vec(), new_record, entry_ptr);
            node
          }
        };

        let split = match node.split_if_needed() {
          Some(split) => split,
          None => {
            self.0.serialize_and_log(slot, &node.to_node(), table)?;
            return Ok(state = LoopState::Break);
          }
        };

        let mid_key = split.top().clone();
        let split_ptr = self.0.alloc_and_log(&split.to_node(), table)?;

        node.set_next(split_ptr);
        self.0.serialize_and_log(slot, &node.to_node(), table)?;
        Ok(state = LoopState::Split(mid_key, split_ptr))
      })?;

      match state {
        LoopState::Break => return Ok(()),
        LoopState::Split(k, p) => return self.propagate_split(k, p, stack, table),
        LoopState::Conflict(i) => {
          self.0.wait_close(i);
          return Err(Error::WriteConflict);
        }
        LoopState::Continue => continue,
      }
    }
  }
  pub fn insert_record(
    &self,
    key: StaticKey,
    record: Option<Vec<u8>>,
    table: &TableHandleRef,
  ) -> Result {
    self.__insert(&key, record, table, true)
  }
  pub fn insert(&self, key: StaticKey, data: Vec<u8>, table: &TableHandleRef) -> Result {
    self.insert_record(key, Some(data), table)
  }
  pub fn remove(&self, key: StaticKeyRef, table: &TableHandleRef) -> Result {
    self.insert_record_if_matched(key, None, table)
  }

  pub fn insert_if_matched(
    &self,
    key: StaticKeyRef,
    data: Option<Vec<u8>>,
    table: &TableHandleRef,
  ) -> Result {
    self.insert_record_if_matched(key, data, table)
  }

  fn insert_record_if_matched(
    &self,
    key: StaticKeyRef,
    record: Option<Vec<u8>>,
    table: &TableHandleRef,
  ) -> Result {
    self.__insert(key, record, table, false)
  }
}

enum LoopState {
  Continue,
  Break,
  Conflict(TxId),
  Split(StaticKey, Pointer),
}

enum Buffered {
  Data(VecRef),
  Chunked(Vec<Pointer>),
}
impl Buffered {
  fn with_versions(
    slot: &ReadonlySlot,
    record: VersionRecordView,
  ) -> Option<(Self, TxId, TxId)> {
    match record.data {
      RecordDataView::Data(s, e) => Some((
        Buffered::Data(VecRef::refed(slot.page(), s, e)),
        record.owner,
        record.version,
      )),
      RecordDataView::Chunked(pointers) => {
        Some((Buffered::Chunked(pointers), record.owner, record.version))
      }
      RecordDataView::Tombstone => None,
    }
  }

  fn from(slot: &ReadonlySlot, record: VersionRecordView) -> Option<Self> {
    match record.data {
      RecordDataView::Data(s, e) => {
        Some(Buffered::Data(VecRef::refed(slot.page(), s, e)))
      }
      RecordDataView::Chunked(pointers) => Some(Buffered::Chunked(pointers)),
      RecordDataView::Tombstone => None,
    }
  }
}

pub struct KVSnapshot {
  key: VecRef,
  value: VecRef,
  owner: TxId,
  version: TxId,
}

pub struct Snapshotter<Policy> {
  policy: Policy,
  table: TableHandleRef,
  buffered: VecDeque<(VecRef, Option<(Buffered, TxId, TxId)>)>,
  next: Option<Pointer>,
  closed: bool,
}
impl<Policy: ReadonlyPolicy> Snapshotter<Policy> {
  fn open(policy: Policy, table: &TableHandleRef) -> Result<Self> {
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
        BTreeNodeView::Internal(node) => ptr = node.first_child()?,
        BTreeNodeView::Leaf(node) => {
          let mut iter = node.get_entries();
          while let Some((s, e, record, p)) = iter.try_next()? {
            if policy.is_visible(record.owner, record.version) {
              buffered.push_back((
                VecRef::refed(slot.page(), s, e),
                Buffered::with_versions(&slot, record),
              ));
              continue;
            }

            if let Some(found) = Self::__find(&policy, table, p)? {
              buffered.push_back((VecRef::refed(slot.page(), s, e), found));
            };
          }

          let mut next = None;
          if iter.is_completed() {
            next = node.get_next();
          }

          return Ok(Self {
            policy,
            table: table.clone(),
            buffered,
            next,
            closed: false,
          });
        }
      }
    }
  }
  fn __find(
    policy: &Policy,
    table: &TableHandleRef,
    ptr: Pointer,
  ) -> Result<Option<Option<(Buffered, TxId, TxId)>>> {
    let mut next = Some(ptr);

    let mut _guard = None;
    while let Some(ptr) = next.take() {
      let new_guard = pin();
      let slot = policy.fetch_slot(ptr, table)?.for_read();
      let entry: DataEntryView = slot.as_ref().view()?;

      if let Some(record) =
        entry.find(|record| policy.is_visible(record.owner, record.version))?
      {
        return Ok(Some(Buffered::with_versions(&slot, record)));
      }

      next = entry.get_next();
      _guard = Some(new_guard);
    }

    Ok(None)
  }

  fn find_value(&self, ptr: Pointer) -> Result<Option<Option<(Buffered, TxId, TxId)>>> {
    Self::__find(&self.policy, &self.table, ptr)
  }

  fn next_record(&mut self) -> Result<Option<(VecRef, Option<(VecRef, TxId, TxId)>)>> {
    loop {
      if self.closed {
        return Ok(None);
      }

      if let Some((key, found)) = self.buffered.pop_front() {
        return Ok(Some(match found {
          Some((Buffered::Data(data), o, v)) => (key, Some((data, o, v))),
          Some((Buffered::Chunked(pointers), o, v)) => (
            key,
            Some((
              BTreeIndex::read_chunk(&self.policy, &pointers, &self.table)?,
              o,
              v,
            )),
          ),
          None => (key, None),
        }));
      }

      self.fill_up()?;
    }
  }

  pub fn is_done(&self) -> bool {
    self.closed
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

    let mut iter = node.get_entries();
    while let Some((s, e, record, p)) = iter.try_next()? {
      if self.policy.is_visible(record.owner, record.version) {
        self.buffered.push_back((
          VecRef::refed(slot.page(), s, e),
          Buffered::with_versions(&slot, record),
        ));
        continue;
      }

      if let Some(found) = self.find_value(p)? {
        self
          .buffered
          .push_back((VecRef::refed(slot.page(), s, e), found));
      };
    }

    if iter.is_completed() {
      self.next = node.get_next();
    }
    Ok(())
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
}

pub struct BTreeIterator<Policy> {
  policy: Policy,
  table: TableHandleRef,
  buffered: VecDeque<(VecRef, Option<Buffered>)>,
  next: Option<Pointer>,
  end: Bound<StaticKey>,
  closed: bool,
}
impl<Policy> BTreeIterator<Policy>
where
  Policy: ReadonlyPolicy,
{
  fn open(
    policy: Policy,
    table: &TableHandleRef,
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
          Bound::Included(k) => ptr = node.find(k)?.unwrap_or_else(|i| i),
          Bound::Excluded(k) => ptr = node.find(k)?.unwrap_or_else(|i| i),
          Bound::Unbounded => ptr = node.first_child()?,
        },
        BTreeNodeView::Leaf(node) => {
          let mut iter = node.range_entries(start, end);
          while let Some((s, e, record, p)) = iter.try_next()? {
            if policy.is_visible(record.owner, record.version) {
              buffered.push_back((
                VecRef::refed(slot.page(), s, e),
                Buffered::from(&slot, record),
              ));
              continue;
            }

            if let Some(found) = Self::__find(&policy, table, p)? {
              buffered.push_back((VecRef::refed(slot.page(), s, e), found));
            };
          }

          let mut next = None;
          if iter.is_completed() {
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

  fn __find(
    policy: &Policy,
    table: &TableHandleRef,
    ptr: Pointer,
  ) -> Result<Option<Option<Buffered>>> {
    let mut next = Some(ptr);

    let mut _guard = None;
    while let Some(ptr) = next.take() {
      let new_guard = pin();
      let slot = policy.fetch_slot(ptr, table)?.for_read();
      let entry: DataEntryView = slot.as_ref().view()?;

      if let Some(record) =
        entry.find(|record| policy.is_visible(record.owner, record.version))?
      {
        return Ok(Some(Buffered::from(&slot, record)));
      }

      next = entry.get_next();
      _guard = Some(new_guard);
    }

    Ok(None)
  }

  fn find_value(&self, ptr: Pointer) -> Result<Option<Option<Buffered>>> {
    Self::__find(&self.policy, &self.table, ptr)
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

    let mut iter = node.range_entries(&Bound::Unbounded, &self.end);
    while let Some((s, e, record, p)) = iter.try_next()? {
      if self.policy.is_visible(record.owner, record.version) {
        self.buffered.push_back((
          VecRef::refed(slot.page(), s, e),
          Buffered::from(&slot, record),
        ));
        continue;
      }

      if let Some(found) = self.find_value(p)? {
        self
          .buffered
          .push_back((VecRef::refed(slot.page(), s, e), found));
      };
    }

    if iter.is_completed() {
      self.next = node.get_next();
    }
    Ok(())
  }

  fn next_record(&mut self) -> Result<Option<(VecRef, Option<VecRef>)>> {
    loop {
      if self.closed {
        return Ok(None);
      }

      if let Some((key, found)) = self.buffered.pop_front() {
        return Ok(Some(match found {
          Some(Buffered::Data(data)) => (key, Some(data)),
          Some(Buffered::Chunked(pointers)) => (
            key,
            Some(BTreeIndex::read_chunk(
              &self.policy,
              &pointers,
              &self.table,
            )?),
          ),
          None => (key, None),
        }));
      }

      self.fill_up()?;
    }
  }
}
impl<Policy: ReadonlyPolicy> MergeSortable for BTreeIterator<Policy> {
  fn try_next(&mut self) -> Result<Option<(VecRef, Option<VecRef>)>> {
    self.next_record()
  }
}
