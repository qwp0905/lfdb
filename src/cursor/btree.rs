use std::{collections::VecDeque, mem::replace, ops::Bound};

use crate::{
  cache::ReadonlySlot,
  disk::Pointer,
  objects::{
    BTreeNode, BTreeNodeView, DataChunk, DataEntry, FindSlotResult, InternalNode,
    NodeFindResult, RecordData, RecordDataView, StaticKey, StaticKeyRef, TreeHeader,
    TreeHeight, VersionRecord, VersionRecordView, CHUNK_SIZE, LARGE_VALUE,
  },
  table::TableHandleRef,
  wal::TxId,
  Error, Result, VecRef,
};

use crossbeam::epoch::pin;

use super::{
  CreatablePolicy, MergeSortable, ReadonlyPolicy, WritablePolicy, HEADER_POINTER,
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
      let obj = slot.as_ref().view()?;
      let chunk = obj.as_data_chunk()?;
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
      .view()?
      .as_tree_header()?
      .get_root();

    loop {
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      let obj = slot.as_ref().view()?;
      match obj.as_btree_node()? {
        BTreeNodeView::Internal(node) => ptr = node.find(key).unwrap_or_else(|i| i),
        BTreeNodeView::Leaf(node) => match node.find(key) {
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
      let obj = slot.as_ref().view()?;
      let entry = obj.as_data_entry()?;

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

  pub fn key_count(&self, table: &TableHandleRef) -> Result<(usize, usize)> {
    let mut ptr = self
      .0
      .fetch_slot(HEADER_POINTER, table)?
      .for_read()
      .as_ref()
      .view()?
      .as_tree_header()?
      .get_root();

    let mut total = 0;
    let mut dead = 0;
    loop {
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      let obj = slot.as_ref().view()?;
      match obj.as_btree_node()? {
        BTreeNodeView::Internal(node) => ptr = node.first_child(),
        BTreeNodeView::Leaf(node) => {
          for (_, _, record, _) in node.get_entries() {
            total += 1;
            if record.data.is_tombstone()
              || !self.0.is_visible(record.owner, record.version)
            {
              dead += 1;
            }
          }

          match node.get_next() {
            Some(i) => ptr = i,
            None => return Ok((total, dead)),
          }
        }
      }
    }
  }

  pub fn contains(&self, key: StaticKeyRef, table: &TableHandleRef) -> Result<bool> {
    let mut ptr = self
      .0
      .fetch_slot(HEADER_POINTER, table)?
      .for_read()
      .as_ref()
      .view()?
      .as_tree_header()?
      .get_root();

    loop {
      // This guard protects the next node or data entry from the GC.
      // By declaring a guard before reading the current page, it is guaranteed that the pointers written to the current page have been reclaimed and are not reused.
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      let obj = slot.as_ref().view()?;
      match obj.as_btree_node()? {
        BTreeNodeView::Internal(node) => ptr = node.find(key).unwrap_or_else(|i| i),
        BTreeNodeView::Leaf(node) => match node.find(key) {
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
      let obj = slot.as_ref().view()?;
      let entry = obj.as_data_entry()?;

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
    table: &TableHandleRef,
  ) -> Result<(Pointer, Vec<Pointer>)> {
    let (mut ptr, height) = {
      let slot = self.0.fetch_slot(HEADER_POINTER, table)?.for_read();
      let obj = slot.as_ref().view()?;
      let header = obj.as_tree_header()?;
      (header.get_root(), header.get_height())
    };
    let mut stack = vec![];

    while let BTreeNodeView::Internal(node) = self
      .0
      .fetch_slot(ptr, table)?
      .for_read()
      .as_ref()
      .view()?
      .as_btree_node()?
    {
      match node.find(&key) {
        Ok(i) => stack.push(replace(&mut ptr, i)),
        Err(i) => ptr = i,
      }
    }

    debug_assert_eq!(height, stack.len() as TreeHeight);
    Ok((ptr, stack))
  }

  pub fn scan(
    &self,
    table: &TableHandleRef,
    start: &Bound<StaticKey>,
    end: &Bound<StaticKey>,
  ) -> Result<BTreeIterator<'_, Policy>> {
    BTreeIterator::open(&self.0, table, start, end)
  }
}

impl<Policy: WritablePolicy> BTreeIndex<Policy> {
  pub fn initialize(&self, table: &TableHandleRef) -> Result {
    let root = self
      .0
      .alloc_and_log(&BTreeNode::initial_state().into(), table)?;

    let mut slot = self.0.alloc_slot(HEADER_POINTER, table)?.for_write();
    self
      .0
      .serialize_and_log(&mut slot, &TreeHeader::new(root).into(), table)?;

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
      self
        .0
        .fetch_slot(ptr, table)?
        .for_batch()
        .mutate(|slot, obj| {
          let internal = obj.as_btree_node_mut()?.as_internal_mut()?;
          if let Err(i) = internal.insert_or_next(&evicted_key, evicted_ptr) {
            return Ok(ptr = i);
          };

          stop = true;
          let (split_node, split_key) = match internal.split_if_needed() {
            Some(v) => v,
            None => {
              self.0.serialize_and_log(slot, &obj, table)?;
              return Ok(());
            }
          };

          let split_index = self.0.alloc_and_log(&split_node.to_node().into(), table)?;
          internal.set_right(&split_key, split_index);
          self.0.serialize_and_log(slot, &obj, table)?;
          result = Some((split_key, split_index));

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
      let old_height = stack.len() as TreeHeight;
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
        .mutate(|header_slot, obj| {
          let header = obj.as_tree_header_mut()?;
          let current_height = header.get_height();
          let ptr = header.get_root();
          if old_height == current_height {
            let new_root =
              InternalNode::initialize(split_key.clone(), ptr, split_pointer);
            let new_root_index =
              self.0.alloc_and_log(&new_root.to_node().into(), table)?;

            header.set_root(new_root_index);
            header.increase_height();
            return self.0.serialize_and_log(header_slot, &obj, table);
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
        let obj = slot.as_ref().view()?;
        let node = obj.as_btree_node()?.as_internal()?;
        match node.find(&split_key) {
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
    while !data.is_empty() {
      let remain = data.split_off(CHUNK_SIZE.min(data.len()));
      let chunk = DataChunk::new(data);
      pointers.push(self.0.alloc_and_log(&chunk.into(), table)?);
      data = remain;
    }

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
      .mutate(|slot, obj| {
        let mut entry = obj.as_data_entry_mut()?;

        let mut new_versions = entry.take_versions().chain([record]).collect::<Vec<_>>();
        new_versions.sort_by(|a, b| b.version.cmp(&a.version));

        loop {
          while let Some(r) = new_versions.pop_if(|r| entry.is_available(r)) {
            entry.append(r);
          }

          if new_versions.is_empty() {
            self.0.serialize_and_log(slot, &obj, table)?;
            break;
          }

          let new_ptr = self.0.alloc_and_log(&obj, table)?;
          let _ = replace(obj, DataEntry::empty(Some(new_ptr)).into());
          entry = obj.as_data_entry_mut()?;
        }

        self.0.after_update_hook(entry_ptr, table);
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
      .mutate(|slot, obj| {
        let entry = obj.as_data_entry_mut()?;

        if entry.is_available(&record) {
          entry.append(record);
          self.0.serialize_and_log(slot, &obj, table)?;
          return Ok(());
        }

        let new_entry_index = self.0.alloc_and_log(&obj, table)?;
        let new_entry = DataEntry::init(record, Some(new_entry_index));
        self.0.serialize_and_log(slot, &new_entry.into(), table)?;

        Ok(())
      })?;

    self.0.after_update_hook(entry_ptr, table);
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
      self
        .0
        .fetch_slot(ptr, table)?
        .for_batch()
        .mutate(|slot, obj| {
          let leaf = obj.as_btree_node_mut()?.as_leaf_mut()?;
          match leaf.find_slot(&key) {
            FindSlotResult::Move(i) => return Ok(ptr = i),
            FindSlotResult::Replace(i, old, entry_ptr) => {
              let is_aborted = self.0.is_aborted(old.owner);
              if old.version > snapshot.version && !is_aborted {
                return Ok(result = Some(Err(entry_ptr)));
              }

              let new_record = record.take().unwrap();
              let old = leaf.replace_at(i, new_record);
              if !is_aborted {
                self.append_version_chain(entry_ptr, old, table)?;
              } else if let RecordData::Chunked(pointers) = old.data {
                pointers.into_iter().for_each(|p| table.free().dealloc(p));
              }
            }
            FindSlotResult::Insert(i) => {
              let entry_ptr = self
                .0
                .alloc_and_log(&DataEntry::empty(None).into(), table)?;
              let new_record = record.take().unwrap();
              leaf.insert_at(i, key.to_vec(), new_record, entry_ptr);
            }
          };

          let split = match leaf.split_if_needed() {
            Some(split) => split,
            None => {
              self.0.serialize_and_log(slot, &obj, table)?;
              return Ok(result = Some(Ok(None)));
            }
          };

          let mid_key = split.top().clone();
          let split_ptr = self.0.alloc_and_log(&split.to_node().into(), table)?;

          leaf.set_next(split_ptr);
          self.0.serialize_and_log(slot, &obj, table)?;
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
  pub fn insert_record(
    &self,
    key: StaticKey,
    mut record: Option<Vec<u8>>,
    table: &TableHandleRef,
  ) -> Result {
    let (mut ptr, stack) = self.find_leaf_stack(&key, table)?;

    loop {
      let mut result = None;
      self
        .0
        .fetch_slot(ptr, table)?
        .for_batch()
        .mutate(|slot, obj| {
          let leaf = obj.as_btree_node_mut()?.as_leaf_mut()?;
          match leaf.find_slot(&key) {
            FindSlotResult::Move(i) => return Ok(ptr = i),
            FindSlotResult::Replace(i, old, entry_ptr) => {
              if self.0.is_conflict(old.owner, old.version) {
                return Ok(result = Some(Err(old.owner)));
              }

              let new_record = VersionRecord::new(
                self.0.current_owner(),
                self.0.current_version(),
                self.create_record(record.take(), table)?,
              );
              let old = leaf.replace_at(i, new_record);
              if !self.0.is_aborted(old.owner) {
                self.append_version_chain(entry_ptr, old, table)?;
              } else if let RecordData::Chunked(pointers) = old.data {
                pointers.into_iter().for_each(|p| table.free().dealloc(p));
              }
            }
            FindSlotResult::Insert(i) => {
              let entry_ptr = self
                .0
                .alloc_and_log(&DataEntry::empty(None).into(), table)?;
              let new_record = VersionRecord::new(
                self.0.current_owner(),
                self.0.current_version(),
                self.create_record(record.take(), &table)?,
              );
              leaf.insert_at(i, key.clone(), new_record, entry_ptr);
            }
          };

          let split = match leaf.split_if_needed() {
            Some(split) => split,
            None => {
              self.0.serialize_and_log(slot, &obj, table)?;
              return Ok(result = Some(Ok(None)));
            }
          };

          let mid_key = split.top().clone();
          let split_ptr = self.0.alloc_and_log(&split.to_node().into(), table)?;

          leaf.set_next(split_ptr);
          self.0.serialize_and_log(slot, &obj, table)?;
          Ok(result = Some(Ok(Some((mid_key, split_ptr)))))
        })?;

      match result {
        Some(Ok(Some((k, p)))) => return self.propagate_split(k, p, stack, table),
        Some(Ok(None)) => return Ok(()),
        Some(Err(i)) => {
          self.0.wait_close(i);
          return Err(Error::WriteConflict);
        }
        None => continue,
      }
    }
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
    mut record: Option<Vec<u8>>,
    table: &TableHandleRef,
  ) -> Result {
    let (mut ptr, stack) = self.find_leaf_stack(&key, table)?;

    loop {
      let mut result = None;
      self
        .0
        .fetch_slot(ptr, table)?
        .for_batch()
        .mutate(|slot, obj| {
          let leaf = obj.as_btree_node_mut()?.as_leaf_mut()?;
          match leaf.find_slot(&key) {
            FindSlotResult::Move(i) => return Ok(ptr = i),
            FindSlotResult::Replace(i, old, entry_ptr) => {
              if self.0.is_conflict(old.owner, old.version) {
                return Ok(result = Some(Err(old.owner)));
              }

              let new_record = VersionRecord::new(
                self.0.current_owner(),
                self.0.current_version(),
                self.create_record(record.take(), table)?,
              );
              let old = leaf.replace_at(i, new_record);
              if !self.0.is_aborted(old.owner) {
                self.append_version_chain(entry_ptr, old, table)?;
              } else if let RecordData::Chunked(pointers) = old.data {
                pointers.into_iter().for_each(|p| table.free().dealloc(p));
              }
            }
            FindSlotResult::Insert(_) => return Ok(result = Some(Ok(None))),
          };

          let split = match leaf.split_if_needed() {
            Some(split) => split,
            None => {
              self.0.serialize_and_log(slot, &obj, table)?;
              return Ok(result = Some(Ok(None)));
            }
          };

          let mid_key = split.top().clone();
          let split_ptr = self.0.alloc_and_log(&split.to_node().into(), table)?;

          leaf.set_next(split_ptr);
          self.0.serialize_and_log(slot, &obj, table)?;
          Ok(result = Some(Ok(Some((mid_key, split_ptr)))))
        })?;

      match result {
        Some(Ok(Some((k, p)))) => return self.propagate_split(k, p, stack, table),
        Some(Ok(None)) => return Ok(()),
        Some(Err(i)) => {
          self.0.wait_close(i);
          return Err(Error::WriteConflict);
        }
        None => continue,
      }
    }
  }
}

enum Buffered {
  Data(VecRef),
  Chunked(Vec<Pointer>),
}
impl Buffered {
  fn from(slot: &ReadonlySlot, record: &VersionRecordView) -> Option<(Self, TxId, TxId)> {
    match &record.data {
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
    }
  }
}

pub struct KVSnapshot {
  key: VecRef,
  value: VecRef,
  owner: TxId,
  version: TxId,
}

pub struct BTreeIterator<'a, Policy> {
  policy: &'a Policy,
  table: TableHandleRef,
  buffered: VecDeque<(VecRef, Option<(Buffered, TxId, TxId)>)>,
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
    table: &TableHandleRef,
    start: &Bound<StaticKey>,
    end: &Bound<StaticKey>,
  ) -> Result<Self> {
    let mut ptr = policy
      .fetch_slot(HEADER_POINTER, table)?
      .for_read()
      .as_ref()
      .view()?
      .as_tree_header()?
      .get_root();

    let mut buffered = VecDeque::new();

    loop {
      let slot = policy.fetch_slot(ptr, table)?.for_read();
      let obj = slot.as_ref().view()?;
      match obj.as_btree_node()? {
        BTreeNodeView::Internal(node) => match &start {
          Bound::Included(k) => ptr = node.find(k).unwrap_or_else(|i| i),
          Bound::Excluded(k) => ptr = node.find(k).unwrap_or_else(|i| i),
          Bound::Unbounded => ptr = node.first_child(),
        },
        BTreeNodeView::Leaf(node) => {
          let pos = match &start {
            Bound::Included(k) => match node.find(k) {
              NodeFindResult::Found(i, _, _) => i,
              NodeFindResult::NotFound(i) => i,
              NodeFindResult::Move(i) => {
                ptr = i;
                continue;
              }
            },
            Bound::Excluded(k) => match node.find(k) {
              NodeFindResult::Found(i, _, _) => i + 1,
              NodeFindResult::NotFound(i) => i,
              NodeFindResult::Move(i) => {
                ptr = i;
                continue;
              }
            },
            Bound::Unbounded => 0,
          };

          let mut count = 0;
          for (s, e, record, p) in node.get_entries_while(end).skip(pos) {
            count += 1;
            if policy.is_visible(record.owner, record.version) {
              buffered.push_back((
                VecRef::refed(slot.page(), s, e),
                Buffered::from(&slot, record),
              ));
              continue;
            }

            if let Some(found) = Self::__find(policy, table, p)? {
              buffered.push_back((VecRef::refed(slot.page(), s, e), found));
            };
          }

          let mut next = None;
          if count == node.len() - pos {
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
  ) -> Result<Option<Option<(Buffered, TxId, TxId)>>> {
    let mut next = Some(ptr);

    let mut _guard = None;
    while let Some(ptr) = next.take() {
      let new_guard = pin();
      let slot = policy.fetch_slot(ptr, table)?.for_read();
      let obj = slot.as_ref().view()?;
      let entry = obj.as_data_entry()?;

      if let Some(record) =
        entry.find(|record| policy.is_visible(record.owner, record.version))
      {
        return Ok(Some(Buffered::from(&slot, record)));
      }

      next = entry.get_next();
      _guard = Some(new_guard);
    }

    Ok(None)
  }

  fn find_value(&self, ptr: Pointer) -> Result<Option<Option<(Buffered, TxId, TxId)>>> {
    Self::__find(self.policy, &self.table, ptr)
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
    let obj = slot.as_ref().view()?;
    let node = obj.as_btree_node()?.as_leaf()?;

    let mut count = 0;
    for (s, e, record, p) in node.get_entries_while(&self.end) {
      count += 1;
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

    if count == node.len() {
      self.next = node.get_next();
    }
    Ok(())
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
              BTreeIndex::read_chunk(self.policy, &pointers, &self.table)?,
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
impl<'a, Policy: ReadonlyPolicy> MergeSortable for BTreeIterator<'a, Policy> {
  fn try_next(&mut self) -> Result<Option<(VecRef, Option<VecRef>)>> {
    match self.next_record()? {
      Some((k, v)) => Ok(Some((k, v.map(|v| v.0)))),
      None => Ok(None),
    }
  }
}
