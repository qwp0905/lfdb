use std::{mem::replace, ops::Bound};

use crate::{
  blob::BlobAppendGuard,
  disk::Pointer,
  objects::{
    BTreeNode, BTreeNodeView, DataEntry, DataEntryView, FindSlotResult, InternalNode,
    NodeFindResult, RecordData, RecordDataView, StaticKey, StaticKeyRef, TreeHeader,
    VersionRecord, HEADER_POINTER, LARGE_VALUE,
  },
  table::{ReserveGuard, TableHandleRef},
  wal::TxId,
  Error, Result,
};

use super::{
  BTreeIter, BTreeRevIter, BufferedValue, CreatablePolicy, KVSnapshot, ReadonlyPolicy,
  ResolvedConflict, Snapshotter, VecRef, WritablePolicy,
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
  pub fn get(&self, key: StaticKeyRef, table: &TableHandleRef) -> Result<GetResult> {
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
          NodeFindResult::NotFound(_) => return Ok(GetResult::Absent),
          NodeFindResult::Move(next) => ptr = next,
          NodeFindResult::Found(_, record, entry_ptr) => {
            if !self.0.is_visible(record.owner, record.version) {
              match entry_ptr {
                Some(p) => break ptr = p,
                None => return Ok(GetResult::Deleted),
              }
            }
            return Ok(match record.data {
              RecordDataView::Data(range) => {
                GetResult::Present(VecRef::refed(slot, range))
              }
              RecordDataView::Blob(id, offset, len) => {
                GetResult::Present(VecRef::copied(self.0.read_blob(id, offset, len)?))
              }
              RecordDataView::Tombstone => GetResult::Deleted,
            });
          }
        },
      }
    }

    let mut next = Some(ptr);
    while let Some(ptr) = next.take() {
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      let entry: DataEntryView = slot.as_ref().view()?;

      if let Some(record) =
        entry.find(|record| self.0.is_visible(record.owner, record.version))?
      {
        return Ok(match record.data {
          RecordDataView::Data(range) => GetResult::Present(VecRef::refed(slot, range)),
          RecordDataView::Blob(id, offset, len) => {
            GetResult::Present(VecRef::copied(self.0.read_blob(id, offset, len)?))
          }
          RecordDataView::Tombstone => GetResult::Deleted,
        });
      }

      next = entry.get_next();
    }

    Ok(GetResult::Absent)
  }

  pub fn lookup(
    &self,
    key: StaticKeyRef,
    table: &TableHandleRef,
  ) -> Result<LookupResult> {
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
          NodeFindResult::NotFound(_) => return Ok(LookupResult::Absent),
          NodeFindResult::Move(next) => ptr = next,
          NodeFindResult::Found(_, record, entry_ptr) => {
            if self.0.is_visible(record.owner, record.version) {
              if record.data.is_tombstone() {
                return Ok(LookupResult::Deleted);
              }
              return Ok(LookupResult::Present);
            }
            match entry_ptr {
              Some(p) => break ptr = p,
              None => return Ok(LookupResult::Absent),
            }
          }
        },
      }
    }

    let mut next = Some(ptr);
    while let Some(ptr) = next.take() {
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      let entry: DataEntryView = slot.as_ref().view()?;
      if let Some(record) =
        entry.find(|record| self.0.is_visible(record.owner, record.version))?
      {
        if record.data.is_tombstone() {
          return Ok(LookupResult::Deleted);
        }
        return Ok(LookupResult::Present);
      };

      next = entry.get_next();
    }

    Ok(LookupResult::Absent)
  }

  pub fn contains(&self, key: StaticKeyRef, table: &TableHandleRef) -> Result<bool> {
    Ok(matches!(self.lookup(key, table)?, LookupResult::Present))
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

  pub fn range(
    &self,
    table: &TableHandleRef,
    start: &Bound<StaticKey>,
    end: &Bound<StaticKey>,
  ) -> Result<BTreeIter<&'_ Policy>> {
    BTreeIter::open(&self.0, table, start, end)
  }
  pub fn range_rev(
    &self,
    table: &TableHandleRef,
    start: &Bound<StaticKey>,
    end: &Bound<StaticKey>,
  ) -> Result<BTreeRevIter<&'_ Policy>> {
    BTreeRevIter::open(&self.0, table, start, end)
  }
}

impl<Policy: ReadonlyPolicy + Clone> BTreeIndex<Policy> {
  pub fn snapshot(&self, table: &TableHandleRef) -> Result<Snapshotter<Policy>> {
    Snapshotter::open(self.0.clone(), table)
  }
}

impl<Policy: WritablePolicy + Sync> BTreeIndex<Policy> {
  pub fn initialize(&self, table: &TableHandleRef) -> Result {
    let root = self.0.alloc_and_log(&BTreeNode::initial_state(), table)?;
    let mut slot = self.0.alloc_slot(HEADER_POINTER, table)?.for_write();
    self
      .0
      .serialize_and_log(&mut slot, &TreeHeader::new(root), table)?;
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
    loop {
      let mut slot = self.0.fetch_slot(ptr, table)?.for_write();
      let mut node = slot.as_ref().deserialize::<BTreeNode>()?;
      let internal = node.as_internal_mut()?;
      if let Err(i) = internal.insert_or_next(&evicted_key, evicted_ptr) {
        ptr = i;
        continue;
      };

      let Some((split_node, split_key)) = internal.split_if_needed() else {
        self.0.serialize_and_log(&mut slot, &node, table)?;
        return Ok(None);
      };

      let split_ptr = self.0.alloc_and_log(&split_node.into_node(), table)?;
      internal.set_right(&split_key, split_ptr);
      self.0.serialize_and_log(&mut slot, &node, table)?;

      return Ok(Some((split_key, split_ptr)));
    }
  }

  pub fn recovery_half_split(
    &self,
    mut split_key: StaticKey,
    mut split_pointer: Pointer,
    level: u16,
    table: &TableHandleRef,
  ) -> Result {
    let mut slot = self.0.fetch_slot(HEADER_POINTER, table)?.for_write();
    let mut header: TreeHeader = slot.as_ref().deserialize()?;

    let mut ptr = header.get_root();
    let height = (header.get_height() - level) as usize;
    let mut stack = vec![];

    while stack.len() < height {
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      let node = slot.as_ref().view::<BTreeNodeView>()?.into_internal()?;
      match node.find(&split_key)? {
        Ok(i) => stack.push(replace(&mut ptr, i)),
        Err(i) => ptr = i,
      }
    }

    while let Some(ptr) = stack.pop() {
      let Some((k, p)) = self.apply_split(split_key, split_pointer, ptr, table)? else {
        return Ok(());
      };
      (split_key, split_pointer) = (k, p);
    }

    let new_root = InternalNode::initialize(split_key, header.get_root(), split_pointer);
    let new_root_ptr = self.0.alloc_and_log(&new_root.into_node(), table)?;

    header.set_root(new_root_ptr);
    header.increase_height();
    self.0.serialize_and_log(&mut slot, &header, table)?;
    Ok(())
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

      let (mut ptr, diff) = {
        let mut slot = self.0.fetch_slot(HEADER_POINTER, table)?.for_write();
        let mut header: TreeHeader = slot.as_ref().deserialize()?;
        let current_height = header.get_height();
        let ptr = header.get_root();

        if old_height == current_height {
          let new_root = InternalNode::initialize(split_key.clone(), ptr, split_pointer);
          let new_root_ptr = self.0.alloc_and_log(&new_root.into_node(), table)?;

          header.set_root(new_root_ptr);
          header.increase_height();
          return self.0.serialize_and_log(&mut slot, &header, table);
        }
        (ptr, (current_height - old_height) as usize)
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
    op: WriteOp,
  ) -> Result<(RecordData, Option<BlobAppendGuard<'_>>)> {
    let WriteOp::Insert(data) = op else {
      return Ok((RecordData::Tombstone, None));
    };
    if data.len() <= LARGE_VALUE {
      return Ok((RecordData::Data(data), None));
    }
    let guard = self.0.write_blob(data)?;
    let data = RecordData::Blob(guard.get_id(), guard.get_offset(), guard.get_len());
    Ok((data, Some(guard)))
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
    let mut ptr = entry_ptr;
    loop {
      let mut slot = self.0.fetch_slot(ptr, table)?.for_write();
      let mut entry: DataEntry = slot.as_ref().deserialize()?;
      if entry.is_available(&record) {
        entry.attach_back(record);
        return self.0.serialize_and_log(&mut slot, &entry, table);
      }
      if let Some(next) = entry.get_next() {
        ptr = next;
        continue;
      }

      let new_entry = DataEntry::init(record, None);
      entry.set_next(self.0.alloc_and_log(&new_entry, table)?);
      return self.0.serialize_and_log(&mut slot, &entry, table);
    }
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
    let key = snapshot.key;
    let record = VersionRecord::new(
      snapshot.owner,
      snapshot.version,
      match snapshot.value {
        BufferedValue::Data(data) => RecordData::Data(data.to_vec()),
        BufferedValue::Blob(id, offset, len) => RecordData::Blob(id, offset, len),
      },
    );

    let (mut ptr, stack) = self.find_leaf_stack(&key, table)?;
    let (mut slot, mut node) = loop {
      let mut slot = self.0.fetch_slot(ptr, table)?.for_write();
      let leaf = slot.as_ref().view::<BTreeNodeView>()?.into_leaf()?;
      match leaf.find(&key)? {
        NodeFindResult::Move(next) => {
          ptr = next;
          continue;
        }
        NodeFindResult::Found(pos, old, entry_ptr) => {
          if let Some(p) = entry_ptr {
            drop(slot);
            return self.apply_version_snapshot(p, record, table);
          }
          if self.0.is_aborted(old.owner) {
            let mut node = leaf.into_owned()?;
            node.replace_at(pos, record);
            break (slot, node);
          }
          if table.is_reserved(&key) {
            continue;
          }

          let mut node = leaf.into_owned()?;
          let entry_ptr = self
            .0
            .alloc_and_log(&DataEntry::init(record, None), table)?;
          node.alloc_entry_at(pos, entry_ptr);
          return self
            .0
            .serialize_and_log(&mut slot, &node.into_node(), table);
        }
        NodeFindResult::NotFound(pos) => {
          let mut node = leaf.into_owned()?;
          node.insert_at(pos, key.to_vec(), record);
          break (slot, node);
        }
      };
    };

    let Some(split) = node.split_if_needed() else {
      return self
        .0
        .serialize_and_log(&mut slot, &node.into_node(), table);
    };

    let mid_key = split.top().clone();
    let split_ptr = self.0.alloc_and_log(&split.into_node(), table)?;

    node.set_next(mid_key.clone(), split_ptr);
    self
      .0
      .serialize_and_log(&mut slot, &node.into_node(), table)?;
    drop(slot);
    self.propagate_split(mid_key, split_ptr, stack, table)?;
    Ok(())
  }
}
impl<Policy: CreatablePolicy + Sync> BTreeIndex<Policy> {
  /**
   * Copy the old leaf record into the data-entry version chain.
   */
  fn copy_old_record(
    &self,
    entry_ptr: Pointer,
    old: VersionRecord,
    table: &TableHandleRef,
  ) -> Result {
    let mut slot = self.0.fetch_slot(entry_ptr, table)?.for_write();
    let mut entry: DataEntry = slot.as_ref().deserialize()?;
    if entry.is_available(&old) {
      entry.attach_front(old);
      return self.0.serialize_and_log(&mut slot, &entry, table);
    }

    let new_entry_ptr = self.0.alloc_and_log(&entry, table)?;
    let new_entry = DataEntry::init(old, Some(new_entry_ptr));
    self.0.serialize_and_log(&mut slot, &new_entry, table)?;
    Ok(())
  }

  fn handle_conflict(&self, i: TxId) -> bool {
    match self.0.resolve_conflict(i) {
      ResolvedConflict::DeadLock => true,
      ResolvedConflict::Closed => !self.0.is_aborted(i),
    }
  }

  /**
   * Insert, update, or delete a key through an optimistic two-step write protocol.
   *
   * Updating an existing key must preserve the previous leaf record in the
   * data-entry version chain. To avoid coupling the leaf latch with the data-entry
   * latch, the method first copies the old leaf record into the data entry, then
   * retries the leaf update and replaces the latest record.
   */
  fn __insert(
    &self,
    key: StaticKeyRef,
    op: WriteOp,
    table: &TableHandleRef,
    create: bool,
  ) -> Result<WriteResult> {
    let (mut ptr, stack) = self.find_leaf_stack(key, table)?;
    let (mut node, mut slot, pos, found) = loop {
      let slot = self.0.fetch_slot(ptr, table)?.for_write();
      let leaf = slot.as_ref().view::<BTreeNodeView>()?.into_leaf()?;
      match leaf.find(key)? {
        NodeFindResult::Move(i) => {
          ptr = i;
          continue;
        }
        NodeFindResult::Found(pos, old, entry_ptr) => {
          let writable = self.0.is_owned(old.owner) || self.0.is_aborted(old.owner);
          if writable {
            break (leaf.into_owned()?, slot, pos, true);
          }
          let visible = self.0.is_readable(old.version) && !self.0.is_active(old.owner);
          if !visible {
            drop(slot);
            if !self.handle_conflict(old.owner) {
              continue;
            }
            return Err(Error::WriteConflict);
          }
          match table.reserve(key.to_vec(), self.0.current_owner()) {
            Ok(g) => {
              let old = old.into_owned_with(slot.as_ref());
              drop(slot);
              let cmd = CopyOld::new(g, entry_ptr, old, op);
              return self.copy_and_update(key, ptr, table, stack, cmd);
            }
            Err(i) => {
              drop(slot);
              if !self.handle_conflict(i) {
                continue;
              }
              return Err(Error::WriteConflict);
            }
          }
        }
        NodeFindResult::NotFound(_) if !create => return Ok(WriteResult::not_matched()),
        NodeFindResult::NotFound(pos) => break (leaf.into_owned()?, slot, pos, false),
      };
    };

    let (record, _guard) = self.create_record(op)?;
    let new_record =
      VersionRecord::new(self.0.current_owner(), self.0.current_version(), record);
    let mut result = if found {
      node.replace_at(pos, new_record);
      WriteResult::updated(false)
    } else {
      node.insert_at(pos, key.to_vec(), new_record);
      WriteResult::inserted(false)
    };

    let Some(split) = node.split_if_needed() else {
      return self
        .0
        .serialize_and_log(&mut slot, &node.into_node(), table)
        .map(|_| result);
    };
    result.splitted = true;

    let mid_key = split.top().clone();
    let split_ptr = self.0.alloc_and_log(&split.into_node(), table)?;

    node.set_next(mid_key.clone(), split_ptr);
    self
      .0
      .serialize_and_log(&mut slot, &node.into_node(), table)?;
    drop(slot);
    self.propagate_split(mid_key, split_ptr, stack, table)?;
    Ok(result)
  }

  fn copy_and_update(
    &self,
    key: StaticKeyRef,
    mut leaf_ptr: Pointer,
    table: &TableHandleRef,
    stack: Vec<Pointer>,
    copy_old: CopyOld,
  ) -> Result<WriteResult> {
    let CopyOld {
      insert_guard,
      entry_ptr,
      old_record: old,
      operation,
    } = copy_old;

    let entry_ptr = match entry_ptr {
      Some(p) => self.copy_old_record(p, old, table).map(|_| p)?,
      None => self.0.alloc_and_log(&DataEntry::init(old, None), table)?,
    };

    loop {
      let mut slot = self.0.fetch_slot(leaf_ptr, table)?.for_write();
      let mut node = slot.as_ref().deserialize::<BTreeNode>()?;
      let leaf = node.as_leaf_mut()?;
      let pos = match leaf.find_slot(key) {
        FindSlotResult::Replace(i) => i,
        FindSlotResult::Move(next) => {
          leaf_ptr = next;
          continue;
        }
        FindSlotResult::Insert(_) => unreachable!(),
      };

      let (record, _guard) = self.create_record(operation)?;
      let new_record =
        VersionRecord::new(self.0.current_owner(), self.0.current_version(), record);
      leaf.replace_at(pos, new_record);
      leaf.alloc_entry_at(pos, entry_ptr);

      let Some(split) = leaf.split_if_needed() else {
        self.0.serialize_and_log(&mut slot, &node, table)?;
        return Ok(WriteResult::updated(false));
      };

      let mid_key = split.top().clone();
      let split_ptr = self.0.alloc_and_log(&split.into_node(), table)?;

      leaf.set_next(mid_key.clone(), split_ptr);
      self.0.serialize_and_log(&mut slot, &node, table)?;
      drop(slot);
      drop(insert_guard);
      self.propagate_split(mid_key, split_ptr, stack, table)?;
      return Ok(WriteResult::updated(true));
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
    op: WriteOp,
    table: &TableHandleRef,
  ) -> Result<WriteResult> {
    self.__insert(&key, op, table, true)
  }
  pub fn insert(
    &self,
    key: StaticKey,
    data: Vec<u8>,
    table: &TableHandleRef,
  ) -> Result<WriteResult> {
    self.insert_record(key, WriteOp::Insert(data), table)
  }
  pub fn remove(&self, key: StaticKeyRef, table: &TableHandleRef) -> Result<WriteResult> {
    self.insert_if_matched(key, WriteOp::Remove, table)
  }
  /**
   * Update an existing key only.
   *
   * Returns `not_matched` instead of creating a new key when the key is absent.
   */
  pub fn insert_if_matched(
    &self,
    key: StaticKeyRef,
    op: WriteOp,
    table: &TableHandleRef,
  ) -> Result<WriteResult> {
    self.__insert(key, op, table, false)
  }
}

pub enum LookupResult {
  Absent,
  Deleted,
  Present,
}
pub enum GetResult {
  Absent,
  Deleted,
  Present(VecRef),
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

pub enum WriteOp {
  Insert(Vec<u8>),
  Remove,
}

struct CopyOld<'a> {
  insert_guard: ReserveGuard<'a>,
  entry_ptr: Option<Pointer>,
  old_record: VersionRecord,
  operation: WriteOp,
}
impl<'a> CopyOld<'a> {
  const fn new(
    insert_guard: ReserveGuard<'a>,
    entry_ptr: Option<Pointer>,
    old_record: VersionRecord,
    operation: WriteOp,
  ) -> Self {
    Self {
      insert_guard,
      entry_ptr,
      old_record,
      operation,
    }
  }
}
