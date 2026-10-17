use std::{collections::VecDeque, mem::replace, ops::Bound};

use crate::{
  disk::Pointer,
  objects::{
    BTreeNode, BTreeNodeView, RecordDataView, StaticKey, StaticKeyRef, TreeHeader,
    HEADER_POINTER,
  },
  page::VecRef,
  table::TableHandleRef,
  Result,
};

use super::{
  append_or_reserve_at_leaf, copy_and_update, drain_snapshot_once, fill_stack_from,
  find_record_in_entry, find_record_in_tree, propagate_split, read_header,
  resolve_conflict, AppendOrReserve, BTreeIter, BTreeRevIter, BulkExecutor, BulkOp,
  CreatablePolicy, FindRecordInTree, KVSnapshot, KeyPair, ReadonlyPolicy, Snapshotter,
  WritablePolicy, WriteOp,
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
    let ptr = match find_record_in_tree(&self.0, key, table)? {
      FindRecordInTree::NotFound => return Ok(GetResult::Absent),
      FindRecordInTree::Found(slot, record) => {
        return Ok(match record.data {
          RecordDataView::Data(range) => GetResult::Present(slot.create_ref(range)),
          RecordDataView::Blob(id, offset, len) => {
            GetResult::Present(VecRef::copied(self.0.read_blob(id, offset, len)?))
          }
          RecordDataView::Tombstone => GetResult::Deleted,
        })
      }
      FindRecordInTree::Invisible(Some(ptr)) => ptr,
      FindRecordInTree::Invisible(None) => return Ok(GetResult::Absent),
    };

    let Some((slot, record)) = find_record_in_entry(&self.0, ptr, table)? else {
      return Ok(GetResult::Absent);
    };

    Ok(match record.data {
      RecordDataView::Data(range) => GetResult::Present(slot.create_ref(range)),
      RecordDataView::Blob(id, offset, len) => {
        GetResult::Present(VecRef::copied(self.0.read_blob(id, offset, len)?))
      }
      RecordDataView::Tombstone => GetResult::Deleted,
    })
  }

  pub fn lookup(
    &self,
    key: StaticKeyRef,
    table: &TableHandleRef,
  ) -> Result<LookupResult> {
    let ptr = match find_record_in_tree(&self.0, key, table)? {
      FindRecordInTree::NotFound => return Ok(LookupResult::Absent),
      FindRecordInTree::Found(_, record) if record.data.is_tombstone() => {
        return Ok(LookupResult::Deleted)
      }
      FindRecordInTree::Found(_, _) => return Ok(LookupResult::Present),
      FindRecordInTree::Invisible(Some(ptr)) => ptr,
      FindRecordInTree::Invisible(None) => return Ok(LookupResult::Absent),
    };

    let Some((_, record)) = find_record_in_entry(&self.0, ptr, table)? else {
      return Ok(LookupResult::Absent);
    };

    if record.data.is_tombstone() {
      return Ok(LookupResult::Deleted);
    }
    Ok(LookupResult::Present)
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
    let header = read_header(&self.0, table)?;
    let mut ptr = header.get_root();
    let height = header.get_height();
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
    let mut slot = self.0.alloc_slot(HEADER_POINTER, table)?;
    self
      .0
      .serialize_and_log(slot.as_prepared(), &TreeHeader::new(root), table)?;
    slot.finalize()?;
    Ok(())
  }

  pub fn recovery_half_split(
    &self,
    split_key: StaticKey,
    split_pointer: Pointer,
    level: u16,
    table: &TableHandleRef,
  ) -> Result {
    let (mut ptr, height) = {
      let header = self
        .0
        .fetch_slot(HEADER_POINTER, table)?
        .for_read()
        .as_ref()
        .deserialize::<TreeHeader>()?;

      (header.get_root(), header.get_height() as usize)
    };

    let diff = height - level as usize;
    let mut stack = vec![];
    while stack.len() < diff {
      let slot = self.0.fetch_slot(ptr, table)?.for_read();
      let node = slot.as_ref().view::<BTreeNodeView>()?.into_internal()?;
      match node.find(&split_key)? {
        Ok(i) => stack.push(replace(&mut ptr, i)),
        Err(i) => ptr = i,
      }
    }

    propagate_split(&self.0, split_key, split_pointer, stack, table, height)
  }

  /**
   * Apply a snapshots record produced by the compaction/snapshot path.
   *
   * This is not the normal transaction write path. The caller guarantees that the
   * snapshot record belongs at the end of the key's version chain, so existing
   * records are extended with `attach_back` semantics instead of conflict-checked
   * transaction update semantics.
   */
  pub fn apply_snapshot_bulk(
    &self,
    bulk: Vec<KVSnapshot>,
    table: &TableHandleRef,
  ) -> Result {
    let mut stack = Vec::<(Option<StaticKey>, Pointer)>::new();
    let mut bulk = VecDeque::from(bulk);
    while let Some(snapshot) = bulk.pop_front() {
      let current: StaticKeyRef = &snapshot.key;
      while stack
        .pop_if(|(k, _)| k.as_deref().is_some_and(|k| k <= current))
        .is_some()
      {}

      let start = match stack.pop() {
        Some((_, p)) => p,
        None => read_header(&self.0, table)?.get_root(),
      };
      let ptr = fill_stack_from(&self.0, current, table, start, &mut stack)?;
      let s = stack.iter().map(|(_, p)| *p).collect::<Vec<_>>();
      drain_snapshot_once(&self.0, snapshot, table, ptr, s, &mut bulk)?;
    }
    Ok(())
  }
}
impl<Policy: CreatablePolicy + Sync> BTreeIndex<Policy> {
  /**
   * Insert, update, or delete a key through an optimistic two-step write protocol.
   *
   * Updating an existing key must preserve the previous leaf record in the
   * data-entry version chain. To avoid coupling the leaf latch with the data-entry
   * latch, the method first copies the old leaf record into the data entry, then
   * retries the leaf update and replaces the latest record.
   */
  fn insert_internal(
    &self,
    key: StaticKey,
    op: WriteOp,
    table: &TableHandleRef,
    create: bool,
  ) -> Result<WriteResult> {
    let (mut ptr, stack) = self.find_leaf_stack(&key, table)?;
    let mut pair = KeyPair(key, op, create);
    loop {
      match append_or_reserve_at_leaf(&self.0, ptr, pair, table, None)? {
        AppendOrReserve::Move(p, kp) => (ptr, pair) = (p, kp),
        AppendOrReserve::Conflict { owner, resume, .. } => {
          resolve_conflict(&self.0, owner)?;
          pair = resume;
        }
        AppendOrReserve::Done {
          mut copy_old,
          mut splitted,
          ..
        } => {
          debug_assert!(copy_old.len() + splitted.len() <= 1);
          if let Some(cmd) = copy_old.pop() {
            return copy_and_update(&self.0, ptr, table, stack, vec![cmd])
              .map(|c| WriteResult::new(c > 0));
          }
          if let Some((k, p)) = splitted.pop() {
            let height = stack.len();
            propagate_split(&self.0, k, p, stack, table, height)?;
            return Ok(WriteResult::new(true));
          }
          return Ok(WriteResult::new(false));
        }
      };
    }
  }

  pub fn insert(
    &self,
    key: StaticKey,
    data: Vec<u8>,
    table: &TableHandleRef,
  ) -> Result<WriteResult> {
    self.insert_internal(key, WriteOp::Insert(data), table, true)
  }
  pub fn remove(&self, key: StaticKeyRef, table: &TableHandleRef) -> Result<WriteResult> {
    self.insert_internal(key.to_vec(), WriteOp::Remove, table, true)
  }
  /**
   * Update an existing key only.
   *
   * Returns `not_matched` instead of creating a new key when the key is absent.
   */
  pub fn remove_if_matched(
    &self,
    key: StaticKeyRef,
    table: &TableHandleRef,
  ) -> Result<WriteResult> {
    self.insert_internal(key.to_vec(), WriteOp::Remove, table, false)
  }
  /**
   * Update an existing key only.
   *
   * Returns `not_matched` instead of creating a new key when the key is absent.
   */
  pub fn insert_if_matched(
    &self,
    key: StaticKeyRef,
    data: Vec<u8>,
    table: &TableHandleRef,
  ) -> Result<WriteResult> {
    self.insert_internal(key.to_vec(), WriteOp::Insert(data), table, false)
  }

  pub fn bulk_executor(
    &self,
    bulk: BulkOp,
    table: &TableHandleRef,
  ) -> BulkExecutor<'_, Policy> {
    BulkExecutor::new(&self.0, bulk.drain_all(), table.clone())
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
  pub splitted: bool,
}
impl WriteResult {
  const fn new(splitted: bool) -> Self {
    Self { splitted }
  }
}
