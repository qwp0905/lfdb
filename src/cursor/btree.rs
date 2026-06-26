use std::{mem::replace, ops::Bound};

use crate::{
  disk::Pointer,
  table::TableHandleRef,
  wal::{TxId, RESERVED_TX},
  Error, Result,
};

use crossbeam::epoch::pin;

use super::{
  BTreeIterator, BTreeNode, BTreeNodeView, BlobAppendGuard, BufferedValue,
  CreatablePolicy, DataEntry, DataEntryView, FindSlotResult, InternalNode, KVSnapshot,
  NodeFindResult, ReadonlyPolicy, RecordData, RecordDataView, Snapshotter, StaticKey,
  StaticKeyRef, TreeHeader, VecRef, VersionRecord, WritablePolicy, HEADER_POINTER,
  LARGE_VALUE,
};

/**
 * Policy-driven B-link tree index implementation.
 *
 * `BTreeIndex` owns the tree access algorithms: traversal, visible-version
 * lookup, insert/update/delete, snapshot application, and split propagation.
 * Concrete transaction, cache, WAL, and blob behavior is supplied by `Policy`,
 * so this layer only knows how to operate the tree structure.
 */
pub struct BTreeIndex<Policy>(Policy);
impl<Policy> BTreeIndex<Policy> {
  pub const fn new(policy: Policy) -> Self {
    Self(policy)
  }
}
impl<Policy: ReadonlyPolicy> BTreeIndex<Policy> {
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
          NodeFindResult::Move(next) => ptr = next,
          NodeFindResult::Found(_, record, entry_ptr) => {
            if !self.0.is_visible(record.owner, record.version) {
              break ptr = entry_ptr;
            }
            return Ok(Some(match &record.data {
              RecordDataView::Data(s, e) => Some(VecRef::refed(slot.page(), *s, *e)),
              RecordDataView::Blob(id, offset, len) => {
                Some(VecRef::copied(self.0.read_blob(*id, *offset, *len)?))
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
          RecordDataView::Blob(id, offset, len) => {
            Some(VecRef::copied(self.0.read_blob(*id, *offset, *len)?))
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
          NodeFindResult::Move(next) => ptr = next,
          NodeFindResult::Found(_, record, entry_ptr) => {
            if !self.0.is_visible(record.owner, record.version) {
              break ptr = entry_ptr;
            }
            return Ok(!record.data.is_tombstone());
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
        return Ok(!record.data.is_tombstone());
      };

      next = entry.get_next();
      _guard = Some(new_guard);
    }

    Ok(false)
  }

  /**
   * The stack stores one internal-node anchor per level, not a perfectly stable
   * parent path. Split propagation rechecks each level and follows B-link right
   * moves again, so the stack only needs enough information to restart propagation
   * from the relevant levels.
   */
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
      match node.find(key)? {
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

  /**
   * Apply one propagated split to an internal level. The stack entry is only an
   * anchor; the function follows B-link right moves until it reaches the node that
   * should receive the separator. If that node splits, return the next separator
   * for the parent level. Otherwise propagation stops.
   */
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
        let mut internal = slot.as_ref().deserialize::<BTreeNode>()?.into_internal()?;
        if let Err(i) = internal.insert_or_next(&evicted_key, evicted_ptr) {
          return Ok(ptr = i);
        };

        stop = true;
        let Some((split_node, split_key)) = internal.split_if_needed() else {
          self
            .0
            .serialize_and_log(slot, &internal.into_node(), table)?;
          return Ok(());
        };

        let split_ptr = self.0.alloc_and_log(&split_node.into_node(), table)?;
        internal.set_right(&split_key, split_ptr);
        self
          .0
          .serialize_and_log(slot, &internal.into_node(), table)?;

        Ok(result = Some((split_key, split_ptr)))
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
        let Some((k, p)) = self.apply_split(split_key, split_pointer, ptr, table)? else {
          return Ok(());
        };

        (split_key, split_pointer) = (k, p);
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
            let new_root_ptr = self.0.alloc_and_log(&new_root.into_node(), table)?;

            header.set_root(new_root_ptr);
            header.increase_height();
            return self.0.serialize_and_log(header_slot, &header, table);
          }

          Ok(changed = Some((ptr, (current_height - old_height) as usize)))
        })?;

      let Some((mut ptr, diff)) = changed else {
        return Ok(());
      };

      while stack.len() < diff {
        let slot = self.0.fetch_slot(ptr, table)?.for_read();
        let node = slot.as_ref().view::<BTreeNodeView>()?.into_internal()?;
        match node.find(&split_key)? {
          Ok(i) => stack.push(replace(&mut ptr, i)),
          Err(i) => ptr = i,
        }
      }
    }
  }

  /**
   * Large values are written to blob storage before the tree record is updated.
   * Keep the append guard alive until the record containing the blob reference has
   * been serialized/logged, so a filled blob segment cannot become readonly and
   * GC-visible in the gap.
   */
  fn create_record(
    &self,
    data: Option<Vec<u8>>,
  ) -> Result<(RecordData, Option<BlobAppendGuard<'_>>)> {
    let Some(data) = data else {
      return Ok((RecordData::Tombstone, None));
    };
    if data.len() <= LARGE_VALUE {
      return Ok((RecordData::Data(data), None));
    }
    let guard = self.0.write_blob(data)?;
    Ok((
      RecordData::Blob(guard.get_id(), guard.get_offset(), guard.get_len()),
      Some(guard),
    ))
  }

  /**
   * Append a snapshot version to the end of an existing data-entry chain.
   *
   * The caller guarantees ordering: the supplied record belongs after the records
   * already present for this key.
   */
  fn apply_version_snapshot(
    &self,
    entry_ptr: Pointer,
    record: VersionRecord,
    table: &TableHandleRef,
  ) -> Result {
    let mut record_slot = Some(record);
    let mut ptr = entry_ptr;

    while let Some(record) = record_slot.take() {
      self.0.fetch_slot(ptr, table)?.for_batch().mutate(|slot| {
        let mut entry: DataEntry = slot.as_ref().deserialize()?;
        if entry.is_available(&record) {
          entry.attach_back(record);
          return self.0.serialize_and_log(slot, &entry, table);
        }

        if let Some(next) = entry.get_next() {
          ptr = next;
          record_slot = Some(record);
          return Ok(());
        }

        let new_entry = DataEntry::init(record, None, RESERVED_TX);
        entry.set_next(self.0.alloc_and_log(&new_entry, table)?);
        self.0.serialize_and_log(slot, &entry, table)?;
        Ok(())
      })?;
    }
    Ok(())
  }

  /**
   * Apply a snapshot record produced by the compaction/snapshot path.
   *
   * This is not the normal transaction write path. The caller guarantees that the
   * snapshot record belongs at the end of the key's version chain, so existing
   * records are extended with `attach_back` semantics instead of conflict-checked
   * transaction update semantics.
   */
  pub fn apply_snapshot(&self, snapshot: KVSnapshot, table: &TableHandleRef) -> Result {
    enum State {
      Continue,
      Break,
      Split(StaticKey, Pointer),
      Apply(Pointer),
    }

    let key = snapshot.key;
    let record = VersionRecord::new(
      snapshot.owner,
      snapshot.version,
      match snapshot.value {
        BufferedValue::Data(data) => RecordData::Data(data.into_vec()),
        BufferedValue::Blob(id, offset, len) => RecordData::Blob(id, offset, len),
      },
      snapshot.record_id,
    );
    let (mut ptr, stack) = self.find_leaf_stack(&key, table)?;

    let mut record = Some(record);
    loop {
      let mut state = State::Continue;
      self.0.fetch_slot(ptr, table)?.for_batch().mutate(|slot| {
        let leaf = slot.as_ref().view::<BTreeNodeView>()?.into_leaf()?;
        let mut node = match leaf.find(&key)? {
          NodeFindResult::Move(next) => return Ok(ptr = next),
          NodeFindResult::Found(pos, old, entry_ptr) => {
            if !self.0.is_aborted(old.owner) {
              return Ok(state = State::Apply(entry_ptr));
            }

            let mut node = leaf.into_owned()?;
            node.replace_at(pos, record.take().unwrap());
            node
          }
          NodeFindResult::NotFound(pos) => {
            let mut node = leaf.into_owned()?;
            let entry_ptr = self.0.alloc_and_log(&DataEntry::empty(), table)?;
            let new_record = record.take().unwrap();
            node.insert_at(pos, key.to_vec(), new_record, entry_ptr);
            node
          }
        };

        let Some(split) = node.split_if_needed() else {
          self.0.serialize_and_log(slot, &node.into_node(), table)?;
          return Ok(state = State::Break);
        };

        let mid_key = split.top().clone();
        let split_ptr = self.0.alloc_and_log(&split.into_node(), table)?;

        node.set_next(split_ptr);
        self.0.serialize_and_log(slot, &node.into_node(), table)?;
        Ok(state = State::Split(mid_key, split_ptr))
      })?;

      match state {
        State::Continue => continue,
        State::Break => return Ok(()),
        State::Split(k, p) => return self.propagate_split(k, p, stack, table),
        State::Apply(i) => {
          return self.apply_version_snapshot(i, record.take().unwrap(), table)
        }
      }
    }
  }
}
impl<Policy> BTreeIndex<Policy>
where
  Policy: CreatablePolicy,
{
  /**
   * Copy the old leaf record into the data-entry version chain.
   *
   * The old record is identified by `(owner, record_id)`. If the same record is
   * already the latest data-entry record, the copy was already performed. In that
   * case the copy is accepted only when this transaction was the last modifier of
   * the data entry; otherwise the last modifier is returned as a conflict owner.
   *
   * If the record has not been copied yet, this method prepends it to the data
   * entry chain and records the current transaction as the last modifier.
   */
  fn copy_old_record(
    &self,
    entry_ptr: Pointer,
    old: VersionRecord,
    table: &TableHandleRef,
  ) -> Result<Option<TxId>> {
    let mut conflict = None;

    self
      .0
      .fetch_slot(entry_ptr, table)?
      .for_batch()
      .mutate(|slot| {
        let entry: DataEntryView = slot.as_ref().view()?;
        if let Some(latest) = entry.get_latest()? {
          if latest.owner == old.owner && latest.record_id == old.record_id {
            conflict =
              (!self.0.is_owned(entry.get_last_owner())).then(|| entry.get_last_owner());
            return Ok(());
          }
        }

        let mut entry = entry.into_owned()?;
        if entry.is_available(&old) {
          entry.attach_front(old, self.0.current_owner());
          self.0.serialize_and_log(slot, &entry, table)?;
          return Ok(());
        }

        let new_entry_ptr = self.0.alloc_and_log(&entry, table)?;
        let new_entry = DataEntry::init(old, Some(new_entry_ptr), self.0.current_owner());
        self.0.serialize_and_log(slot, &new_entry, table)?;
        Ok(())
      })?;

    Ok(conflict)
  }

  /**
   * Insert, update, or delete a key through an optimistic two-step write protocol.
   *
   * Updating an existing key must preserve the previous leaf record in the
   * data-entry version chain. To avoid coupling the leaf latch with the data-entry
   * latch, the method first copies the old leaf record into the data entry, then
   * retries the leaf update and replaces the latest record. The copied record is
   * checked by `(owner, record_id)`, and data-entry `last_owner` is used to detect
   * whether another transaction performed the copy first.
   */
  fn __insert(
    &self,
    key: StaticKeyRef,
    mut record: Option<Vec<u8>>,
    table: &TableHandleRef,
    create: bool,
  ) -> Result<WriteResult> {
    enum State {
      Continue,
      Break(WriteResult),
      Conflict(TxId),
      Split(StaticKey, Pointer, WriteResult),
      CopyOld(Pointer, VersionRecord),
    }

    let (mut ptr, stack) = self.find_leaf_stack(key, table)?;
    let mut _guard = None;

    loop {
      let mut state = State::Continue;
      self.0.fetch_slot(ptr, table)?.for_batch().mutate(|slot| {
        let leaf = slot.as_ref().view::<BTreeNodeView>()?.into_leaf()?;
        let (mut node, mut result) = match leaf.find(key)? {
          NodeFindResult::Move(pos) => return Ok(ptr = pos),
          NodeFindResult::Found(pos, old, entry_ptr) => {
            if self.0.is_conflict(old.owner, old.version) {
              // Fast-fail before touching the data-entry chain when the latest leaf record
              // already belongs to a conflicting writer/version.
              return Ok(state = State::Conflict(old.owner));
            }

            if !self.0.is_aborted(old.owner) && !self.0.is_owned(old.owner) {
              let old = old.into_owned_with(slot.as_ref());
              return Ok(state = State::CopyOld(entry_ptr, old));
            }

            let mut node = leaf.into_owned()?;
            let (record, guard) = self.create_record(record.take())?;
            debug_assert!(_guard.is_none());
            _guard = guard;
            let new_record = VersionRecord::new(
              self.0.current_owner(),
              self.0.current_version(),
              record,
              self.0.gen_record_id(),
            );

            node.replace_at(pos, new_record);
            (node, WriteResult::updated(false))
          }
          NodeFindResult::NotFound(pos) => {
            if !create {
              return Ok(state = State::Break(WriteResult::not_matched()));
            }

            let mut node = leaf.into_owned()?;
            let entry_ptr = self.0.alloc_and_log(&DataEntry::empty(), table)?;

            let (record, guard) = self.create_record(record.take())?;
            debug_assert!(_guard.is_none());
            _guard = guard;
            let new_record = VersionRecord::new(
              self.0.current_owner(),
              self.0.current_version(),
              record,
              self.0.gen_record_id(),
            );
            node.insert_at(pos, key.to_vec(), new_record, entry_ptr);
            (node, WriteResult::inserted(false))
          }
        };

        let Some(split) = node.split_if_needed() else {
          self.0.serialize_and_log(slot, &node.into_node(), table)?;
          return Ok(state = State::Break(result));
        };
        result.splitted = true;

        let mid_key = split.top().clone();
        let split_ptr = self.0.alloc_and_log(&split.into_node(), table)?;

        node.set_next(split_ptr);
        self.0.serialize_and_log(slot, &node.into_node(), table)?;
        Ok(state = State::Split(mid_key, split_ptr, result))
      })?;

      match state {
        State::Continue => continue,
        State::Break(result) => return Ok(result),
        State::Split(k, p, result) => {
          self.propagate_split(k, p, stack, table)?;
          return Ok(result);
        }
        State::Conflict(i) => {
          self.0.wait_close(i);
          return Err(Error::WriteConflict);
        }
        State::CopyOld(entry_ptr, old) => {
          if let Some(i) = self.copy_old_record(entry_ptr, old, table)? {
            self.0.wait_close(i);
            return Err(Error::WriteConflict);
          };
          break;
        }
      };
    }

    loop {
      let mut state = State::Continue;
      self.0.fetch_slot(ptr, table)?.for_batch().mutate(|slot| {
        let mut leaf = slot.as_ref().deserialize::<BTreeNode>()?.into_leaf()?;
        let pos = match leaf.find_slot(key) {
          FindSlotResult::Replace(i) => i,
          FindSlotResult::Move(next) => return Ok(ptr = next),
          FindSlotResult::Insert(_) => unreachable!(),
        };

        let (record, guard) = self.create_record(record.take())?;
        debug_assert!(_guard.is_none());
        _guard = guard;
        let new_record = VersionRecord::new(
          self.0.current_owner(),
          self.0.current_version(),
          record,
          self.0.gen_record_id(),
        );
        leaf.replace_at(pos, new_record);

        let Some(split) = leaf.split_if_needed() else {
          self.0.serialize_and_log(slot, &leaf.into_node(), table)?;
          return Ok(state = State::Break(WriteResult::updated(false)));
        };

        let mid_key = split.top().clone();
        let split_ptr = self.0.alloc_and_log(&split.into_node(), table)?;

        leaf.set_next(split_ptr);
        self.0.serialize_and_log(slot, &leaf.into_node(), table)?;
        Ok(state = State::Split(mid_key, split_ptr, WriteResult::updated(true)))
      })?;

      match state {
        State::Continue => continue,
        State::Break(result) => return Ok(result),
        State::Split(k, p, result) => {
          self.propagate_split(k, p, stack, table)?;
          return Ok(result);
        }
        _ => unreachable!(),
      }
    }
  }
  /**
   * Insert a version record, creating the key if it does not exist.
   *
   * `None` creates a tombstone record. This is intentionally distinct from
   * `remove`, which only writes a tombstone when the key already exists.
   */
  pub fn insert_record(
    &self,
    key: StaticKey,
    record: Option<Vec<u8>>,
    table: &TableHandleRef,
  ) -> Result<WriteResult> {
    self.__insert(&key, record, table, true)
  }
  pub fn insert(
    &self,
    key: StaticKey,
    data: Vec<u8>,
    table: &TableHandleRef,
  ) -> Result<WriteResult> {
    self.insert_record(key, Some(data), table)
  }
  pub fn remove(&self, key: StaticKeyRef, table: &TableHandleRef) -> Result<WriteResult> {
    self.insert_if_matched(key, None, table)
  }
  /**
   * Update an existing key only.
   *
   * Returns `not_matched` instead of creating a new key when the key is absent.
   */
  pub fn insert_if_matched(
    &self,
    key: StaticKeyRef,
    data: Option<Vec<u8>>,
    table: &TableHandleRef,
  ) -> Result<WriteResult> {
    self.__insert(key, data, table, false)
  }
}

pub struct WriteResult {
  pub inserted: bool,
  pub updated: bool,
  pub splitted: bool,
}
impl WriteResult {
  const fn inserted(splitted: bool) -> Self {
    Self {
      inserted: true,
      updated: false,
      splitted,
    }
  }
  const fn updated(splitted: bool) -> Self {
    Self {
      inserted: false,
      updated: true,
      splitted,
    }
  }
  const fn not_matched() -> Self {
    Self {
      inserted: false,
      updated: false,
      splitted: false,
    }
  }
}
