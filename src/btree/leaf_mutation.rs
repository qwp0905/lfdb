use std::{
  collections::{BTreeSet, VecDeque},
  iter::Peekable,
};

use crate::{
  blob::BlobAppendGuard,
  cache::WritableSlot,
  disk::Pointer,
  objects::{
    BTreeNode, BTreeNodeView, DataEntry, FindSlotResult, LeafNode, NodeFindResult,
    RecordData, StaticKey, StaticKeyRef, VersionRecord, VersionRecordView, LARGE_VALUE,
  },
  table::{ReserveGuard, TableHandleRef},
  wal::TxId,
  Error, Result,
};

use super::{propagate_split, CreatablePolicy, ResolvedConflict, WritablePolicy};

pub enum WriteOp {
  Insert(Vec<u8>),
  Remove,
}

/**
 * Copy the old leaf record into the data-entry version chain.
 */
fn copy_old_record<Policy: WritablePolicy + Sync>(
  policy: &Policy,
  entry_ptr: Pointer,
  old: VersionRecord,
  table: &TableHandleRef,
) -> Result {
  loop {
    let Some(slot) = policy.fetch_slot(entry_ptr, table)? else {
      continue;
    };
    let mut slot = slot.for_write();
    let Some(prepared) = slot.prepare() else {
      continue;
    };
    let mut entry = prepared.as_ref().deserialize::<DataEntry>()?;
    if entry.is_available(&old) {
      entry.attach_front(old);
      policy.serialize_and_log(prepared, &entry, table)?;
      return Ok(());
    }

    let Some(new_entry_ptr) = policy.alloc_and_log(&entry, table)? else {
      continue;
    };
    let new_entry = DataEntry::init(old, Some(new_entry_ptr));
    policy.serialize_and_log(prepared, &new_entry, table)?;
    return Ok(());
  }
}

/**
 * Large values are written to blob storage before the tree record is updated.
 * Keep the append guard alive until the record containing the blob reference has
 * been serialized/logged, so a filled blob segment cannot become readonly and
 * GC-visible in the gap.
 */
fn create_record<Policy: WritablePolicy>(
  policy: &Policy,
  op: WriteOp,
) -> Result<(RecordData, MaybeBlobGuard<'_>)> {
  let WriteOp::Insert(data) = op else {
    return Ok((RecordData::Tombstone, None));
  };
  if data.len() <= LARGE_VALUE {
    return Ok((RecordData::Data(data), None));
  }
  let guard = policy.write_blob(data)?;
  let data = RecordData::Blob(guard.get_id(), guard.get_offset(), guard.get_len());
  Ok((data, Some(guard)))
}

enum CompleteLeafOnce<'a> {
  Move(Option<Pointer>, MaybeWritten<'a>),
  Break(MaybeBlobGuard<'a>),
  Split(StaticKey, Pointer, MaybeBlobGuard<'a>),
}
fn complete_leaf_once<'a, Policy: CreatablePolicy + Sync>(
  policy: &'a Policy,
  slot: Option<&mut WritableSlot>,
  leaf: &mut LeafNode,
  key: StaticKeyRef,
  operation: MaybeWritten<'a>,
  entry_ptr: Pointer,
  table: &TableHandleRef,
) -> Result<CompleteLeafOnce<'a>> {
  let pos = match leaf.find_slot(key) {
    FindSlotResult::Replace(i, _, _) => i,
    FindSlotResult::Move(next) => {
      return Ok(CompleteLeafOnce::Move(Some(next), operation))
    }
    FindSlotResult::Insert(_) => unreachable!(),
  };

  if slot.is_some_and(|slot| slot.prepare().is_none()) {
    return Ok(CompleteLeafOnce::Move(None, operation));
  }

  let (record, guard) = match operation {
    MaybeWritten::Operation(op) => create_record(policy, op)?,
    MaybeWritten::Written(r, g) => (r, g),
  };

  if leaf.is_available(key, &record, Some(pos)) {
    let new_record =
      VersionRecord::new(policy.current_owner(), policy.current_version(), record);
    leaf.replace_at(pos, new_record);
    leaf.alloc_entry_at(pos, entry_ptr);
    return Ok(CompleteLeafOnce::Break(guard));
  };

  let (mid_key, split_ptr) = {
    let Some(slot) = policy.try_alloc_slot(table)? else {
      return Ok(CompleteLeafOnce::Move(
        None,
        MaybeWritten::Written(record, guard),
      ));
    };
    let mut slot = slot.for_write();
    let Some(prepared) = slot.prepare() else {
      table.free().dealloc(slot.get_pointer());
      return Ok(CompleteLeafOnce::Move(
        None,
        MaybeWritten::Written(record, guard),
      ));
    };

    let new_record =
      VersionRecord::new(policy.current_owner(), policy.current_version(), record);
    leaf.replace_at(pos, new_record);
    leaf.alloc_entry_at(pos, entry_ptr);

    let split = leaf.split_node();
    let mid_key = split.top().to_vec();
    policy.serialize_and_log(prepared, &split.into_node(), table)?;
    (mid_key, prepared.get_pointer())
  };
  leaf.set_next(mid_key.clone(), split_ptr);
  Ok(CompleteLeafOnce::Split(mid_key, split_ptr, guard))
}

pub enum MaybeWritten<'a> {
  Operation(WriteOp),
  Written(RecordData, MaybeBlobGuard<'a>),
}

pub enum MaybeWrittenKeyPair<'a> {
  KeyPair(KeyPair),
  Written(StaticKey, MaybeWritten<'a>, bool),
}

struct LeafCompletion<'a> {
  insert_guard: ReserveGuard<'a>,
  entry_ptr: Pointer,
  key: StaticKey,
  operation: MaybeWritten<'a>,
}
impl<'a> LeafCompletion<'a> {
  const fn new(
    insert_guard: ReserveGuard<'a>,
    entry_ptr: Pointer,
    key: StaticKey,
    operation: MaybeWritten<'a>,
  ) -> Self {
    Self {
      insert_guard,
      entry_ptr,
      key,
      operation,
    }
  }
}

fn complete_leaf<'a, Policy: CreatablePolicy + Sync>(
  policy: &'a Policy,
  mut leaf_ptr: Pointer,
  table: &TableHandleRef,
  must_apply: LeafCompletion,
  buffered: &mut VecDeque<LeafCompletion<'a>>,
) -> Result<(Vec<(StaticKey, Pointer)>, Pointer)> {
  let mut splitted = Vec::new();
  let LeafCompletion {
    insert_guard,
    entry_ptr,
    key,
    mut operation,
  } = must_apply;

  loop {
    let Some(slot) = policy.fetch_slot(leaf_ptr, table)? else {
      continue;
    };
    let mut guards = Vec::new();
    let mut slot = slot.for_write();
    let mut node = slot.as_ref().deserialize::<BTreeNode>()?;
    let leaf = node.as_leaf_mut()?;
    match complete_leaf_once(
      policy,
      Some(&mut slot),
      leaf,
      &key,
      operation,
      entry_ptr,
      table,
    )? {
      CompleteLeafOnce::Move(p, op) => {
        (leaf_ptr, operation) = (p.unwrap_or(leaf_ptr), op);
        continue;
      }
      CompleteLeafOnce::Break(guard) => guards.push((insert_guard, guard)),
      CompleteLeafOnce::Split(k, p, guard) => {
        guards.push((insert_guard, guard));
        splitted.push((k, p));
      }
    };

    let Some(prepared) = slot.prepare() else {
      unreachable!()
    };

    while let Some(completion) =
      buffered.pop_front_if(|c| leaf.get_next_key().is_none_or(|r| &*c.key < r))
    {
      let LeafCompletion {
        insert_guard,
        entry_ptr,
        key,
        operation,
      } = completion;
      match complete_leaf_once(policy, None, leaf, &key, operation, entry_ptr, table)? {
        CompleteLeafOnce::Move(_, op) => {
          buffered.push_front(LeafCompletion::new(insert_guard, entry_ptr, key, op));
          break;
        }
        CompleteLeafOnce::Break(guard) => guards.push((insert_guard, guard)),
        CompleteLeafOnce::Split(k, p, guard) => {
          guards.push((insert_guard, guard));
          splitted.push((k, p));
        }
      };
    }
    policy.serialize_and_log(prepared, &node, table)?;
    return Ok((splitted, leaf_ptr));
  }
}

fn create_data_entry_with<Policy: WritablePolicy + Sync>(
  policy: &Policy,
  old: VersionRecord,
  table: &TableHandleRef,
) -> Result<Pointer> {
  loop {
    let Some(slot) = policy.try_alloc_slot(table)? else {
      continue;
    };
    let mut slot = slot.for_write();
    let Some(prepared) = slot.prepare() else {
      table.free().dealloc(slot.get_pointer());
      continue;
    };
    policy.serialize_and_log(prepared, &DataEntry::init(old, None), table)?;
    return Ok(prepared.get_pointer());
  }
}

pub fn copy_and_update<'a, Policy: CreatablePolicy + Sync>(
  policy: &Policy,
  leaf_ptr: Pointer,
  table: &TableHandleRef,
  stack: Vec<Pointer>,
  bulk: Vec<(StaticKey, CopyOld<'a>)>,
) -> Result<usize> {
  let mut records = VecDeque::with_capacity(bulk.len());
  for (key, copy_old) in bulk {
    let CopyOld {
      insert_guard,
      entry_ptr,
      old_record: old,
      operation,
    } = copy_old;
    let entry_ptr = match entry_ptr {
      Some(p) => copy_old_record(policy, p, old, table).map(|_| p)?,
      None => create_data_entry_with(policy, old, table)?,
    };
    records.push_back(LeafCompletion::new(insert_guard, entry_ptr, key, operation));
  }

  let mut splitted = Vec::new();
  let mut ptr = leaf_ptr;
  while let Some(completion) = records.pop_front() {
    let (s, p) = complete_leaf(policy, ptr, table, completion, &mut records)?;
    splitted.extend(s);
    ptr = p;
  }
  let splitted_count = splitted.len();
  for (k, p) in splitted {
    propagate_split(policy, k, p, stack.clone(), table, stack.len())?;
  }
  Ok(splitted_count)
}

pub struct KeyPair(pub StaticKey, pub WriteOp, pub bool);
impl PartialEq for KeyPair {
  fn eq(&self, other: &Self) -> bool {
    self.0.eq(&other.0)
  }
}
impl Eq for KeyPair {}
impl PartialOrd for KeyPair {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(Ord::cmp(self, other))
  }
}
impl Ord for KeyPair {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    Ord::cmp(&self.0, &other.0)
  }
}

pub struct KeyPairList(Peekable<std::collections::btree_set::IntoIter<KeyPair>>);
impl KeyPairList {
  pub fn new(pairs: BTreeSet<KeyPair>) -> Self {
    Self(pairs.into_iter().peekable())
  }
  fn pop_if(&mut self, f: impl FnOnce(StaticKeyRef) -> bool) -> Option<KeyPair> {
    self.0.next_if(|KeyPair(k, _, _)| f(k))
  }
  pub fn pop(&mut self) -> Option<KeyPair> {
    self.0.next()
  }
}

pub struct CopyOld<'a> {
  pub insert_guard: ReserveGuard<'a>,
  pub entry_ptr: Option<Pointer>,
  pub old_record: VersionRecord,
  pub operation: MaybeWritten<'a>,
}
impl<'a> CopyOld<'a> {
  const fn new(
    insert_guard: ReserveGuard<'a>,
    entry_ptr: Option<Pointer>,
    old_record: VersionRecord,
    operation: MaybeWritten<'a>,
  ) -> Self {
    Self {
      insert_guard,
      entry_ptr,
      old_record,
      operation,
    }
  }
}

type MaybeBlobGuard<'a> = Option<BlobAppendGuard<'a>>;

enum ApplyOperation {
  Failed(RecordData),
  Split(StaticKey, Pointer),
  Break,
}

fn apply_operation<'a, Policy: CreatablePolicy + Sync>(
  policy: &'a Policy,
  node: &mut LeafNode,
  key: StaticKeyRef,
  op: MaybeWritten<'a>,
  table: &TableHandleRef,
  pos: usize,
  found: bool,
) -> Result<(ApplyOperation, MaybeBlobGuard<'a>)> {
  let (record, guard) = match op {
    MaybeWritten::Operation(op) => create_record(policy, op)?,
    MaybeWritten::Written(r, g) => (r, g),
  };
  if node.is_available(key, &record, found.then_some(pos)) {
    let new_record =
      VersionRecord::new(policy.current_owner(), policy.current_version(), record);
    if found {
      node.replace_at(pos, new_record);
    } else {
      node.insert_at(pos, key.to_vec(), new_record);
    };
    return Ok((ApplyOperation::Break, guard));
  };

  let (mid_key, split_ptr) = {
    let Some(slot) = policy.try_alloc_slot(table)? else {
      return Ok((ApplyOperation::Failed(record), guard));
    };
    let mut slot = slot.for_write();
    let Some(prepared) = slot.prepare() else {
      table.free().dealloc(slot.get_pointer());
      return Ok((ApplyOperation::Failed(record), guard));
    };

    let new_record =
      VersionRecord::new(policy.current_owner(), policy.current_version(), record);
    if found {
      node.replace_at(pos, new_record);
    } else {
      node.insert_at(pos, key.to_vec(), new_record);
    };

    let split = node.split_node();
    let mid_key = split.top().to_vec();
    policy.serialize_and_log(prepared, &split.into_node(), table)?;
    (mid_key, prepared.get_pointer())
  };

  node.set_next(mid_key.clone(), split_ptr);
  Ok((ApplyOperation::Split(mid_key, split_ptr), guard))
}

enum TryAppendAtLeaf<'a> {
  Failed(MaybeWritten<'a>),
  NotFound,
  Break(MaybeBlobGuard<'a>),
  Conflict(TxId, MaybeWritten<'a>),
  Split(StaticKey, Pointer, MaybeBlobGuard<'a>),
  CopyOld(CopyOld<'a>),
}
fn try_append_at_leaf_once<'a, Policy: CreatablePolicy + Sync>(
  policy: &'a Policy,
  leaf: &mut LeafNode,
  table: &'a TableHandleRef,
  key: StaticKeyRef,
  op: MaybeWritten<'a>,
  create: bool,
  slot: &mut WritableSlot,
) -> Result<TryAppendAtLeaf<'a>> {
  let (pos, found, op) = match leaf.find_slot(key) {
    FindSlotResult::Move(_) => unreachable!(),
    FindSlotResult::Replace(pos, old, entry_ptr) => {
      let writable = policy.is_owned(old.owner) || policy.is_aborted(old.owner);
      let visible = policy.is_readable(old.version) && !policy.is_active(old.owner);
      match (writable, visible) {
        (true, _) => (pos, true, op),
        (false, false) => return Ok(TryAppendAtLeaf::Conflict(old.owner, op)),
        (false, true) => {
          return Ok(match table.reserve(key.to_vec(), policy.current_owner()) {
            Ok(g) => {
              TryAppendAtLeaf::CopyOld(CopyOld::new(g, entry_ptr, old.clone(), op))
            }
            Err(i) => TryAppendAtLeaf::Conflict(i, op),
          })
        }
      }
    }
    FindSlotResult::Insert(pos) => {
      if !create {
        return Ok(TryAppendAtLeaf::NotFound);
      }
      (pos, false, op)
    }
  };

  if slot.prepare().is_none() {
    return Ok(TryAppendAtLeaf::Failed(op));
  }

  match apply_operation(policy, leaf, key, op, table, pos, found)? {
    (ApplyOperation::Failed(r), g) => {
      Ok(TryAppendAtLeaf::Failed(MaybeWritten::Written(r, g)))
    }
    (ApplyOperation::Split(k, p), g) => Ok(TryAppendAtLeaf::Split(k, p, g)),
    (ApplyOperation::Break, g) => Ok(TryAppendAtLeaf::Break(g)),
  }
}

pub enum AppendOrReserve<'a> {
  Move(Pointer, MaybeWrittenKeyPair<'a>),
  Conflict {
    owner: TxId,
    resume: MaybeWrittenKeyPair<'a>,
    copy_old: Vec<(StaticKey, CopyOld<'a>)>,
    splitted: Vec<(StaticKey, Pointer)>,
    _guards: Vec<MaybeBlobGuard<'a>>,
  },
  Done {
    copy_old: Vec<(StaticKey, CopyOld<'a>)>,
    splitted: Vec<(StaticKey, Pointer)>,
    _guards: Vec<MaybeBlobGuard<'a>>,
  },
  Stopped {
    resume: MaybeWrittenKeyPair<'a>,
    copy_old: Vec<(StaticKey, CopyOld<'a>)>,
    splitted: Vec<(StaticKey, Pointer)>,
    _guards: Vec<MaybeBlobGuard<'a>>,
  },
}

enum TryAppendAtLeafInit<'a> {
  NotFound,
  Move(Pointer, MaybeWritten<'a>),
  Conflict(TxId, MaybeWritten<'a>),
  CopyOld(
    ReserveGuard<'a>,
    Option<Pointer>,
    VersionRecordView,
    MaybeWritten<'a>,
  ),
  Break(MaybeBlobGuard<'a>, LeafNode),
  Split(StaticKey, Pointer, MaybeBlobGuard<'a>, LeafNode),
  Failed(MaybeWritten<'a>),
}

fn try_append_at_leaf_init<'a, Policy: CreatablePolicy + Sync>(
  policy: &'a Policy,
  table: &'a TableHandleRef,
  key: StaticKeyRef,
  op: MaybeWritten<'a>,
  create: bool,
  slot: &mut WritableSlot,
) -> Result<TryAppendAtLeafInit<'a>> {
  let leaf = slot.as_ref().view::<BTreeNodeView>()?.into_leaf()?;
  let (mut leaf, pos, found) = match leaf.find(key)? {
    NodeFindResult::Move(p) => return Ok(TryAppendAtLeafInit::Move(p, op)),
    NodeFindResult::Found(pos, old, entry_ptr) => {
      let writable = policy.is_owned(old.owner) || policy.is_aborted(old.owner);
      let visible = policy.is_readable(old.version) && !policy.is_active(old.owner);
      match (writable, visible) {
        (true, _) => (leaf.into_owned()?, pos, true),
        (false, false) => return Ok(TryAppendAtLeafInit::Conflict(old.owner, op)),
        (false, true) => {
          return Ok(match table.reserve(key.to_vec(), policy.current_owner()) {
            Ok(g) => TryAppendAtLeafInit::CopyOld(g, entry_ptr, old, op),
            Err(i) => TryAppendAtLeafInit::Conflict(i, op),
          })
        }
      }
    }
    NodeFindResult::NotFound(pos) => {
      if !create {
        return Ok(TryAppendAtLeafInit::NotFound);
      }
      (leaf.into_owned()?, pos, false)
    }
  };

  if slot.prepare().is_none() {
    return Ok(TryAppendAtLeafInit::Failed(op));
  }

  match apply_operation(policy, &mut leaf, key, op, table, pos, found)? {
    (ApplyOperation::Failed(r), g) => {
      Ok(TryAppendAtLeafInit::Failed(MaybeWritten::Written(r, g)))
    }
    (ApplyOperation::Split(k, p), g) => Ok(TryAppendAtLeafInit::Split(k, p, g, leaf)),
    (ApplyOperation::Break, g) => Ok(TryAppendAtLeafInit::Break(g, leaf)),
  }
}

pub fn append_or_reserve_at_leaf<'a, Policy: CreatablePolicy + Sync>(
  policy: &'a Policy,
  leaf_ptr: Pointer,
  must_apply: MaybeWrittenKeyPair<'a>,
  table: &'a TableHandleRef,
  others: Option<&mut KeyPairList>,
) -> Result<AppendOrReserve<'a>> {
  let Some(slot) = policy.fetch_slot(leaf_ptr, table)? else {
    return Ok(AppendOrReserve::Move(leaf_ptr, must_apply));
  };

  let mut splitted = Vec::new();
  let mut copy_old = Vec::new();
  let mut guards = Vec::new();
  let mut slot = slot.for_write();
  let (key, op, create) = match must_apply {
    MaybeWrittenKeyPair::KeyPair(KeyPair(k, op, c)) => {
      (k, MaybeWritten::Operation(op), c)
    }
    MaybeWrittenKeyPair::Written(k, op, c) => (k, op, c),
  };

  let maybe_owned =
    match try_append_at_leaf_init(policy, table, &key, op, create, &mut slot)? {
      TryAppendAtLeafInit::NotFound => None,
      TryAppendAtLeafInit::Move(p, op) => {
        return Ok(AppendOrReserve::Move(
          p,
          MaybeWrittenKeyPair::Written(key, op, create),
        ))
      }
      TryAppendAtLeafInit::Failed(op) => {
        return Ok(AppendOrReserve::Stopped {
          resume: MaybeWrittenKeyPair::Written(key, op, create),
          copy_old,
          splitted,
          _guards: guards,
        });
      }
      TryAppendAtLeafInit::Conflict(i, op) => {
        return Ok(AppendOrReserve::Conflict {
          owner: i,
          resume: MaybeWrittenKeyPair::Written(key, op, create),
          copy_old,
          splitted,
          _guards: guards,
        })
      }
      TryAppendAtLeafInit::CopyOld(g, ep, old, op) => {
        let cmd = CopyOld::new(g, ep, old.into_owned_with(slot.as_ref()), op);
        copy_old.push((key, cmd));
        None
      }
      TryAppendAtLeafInit::Break(g, l) => {
        guards.push(g);
        Some(l)
      }
      TryAppendAtLeafInit::Split(k, p, g, l) => {
        splitted.push((k, p));
        guards.push(g);
        Some(l)
      }
    };

  let Some(others) = others else {
    if let Some(leaf) = maybe_owned {
      let Some(prepared) = slot.prepare() else {
        unreachable!();
      };
      policy.serialize_and_log(prepared, &leaf.into_node(), table)?;
    }
    return Ok(AppendOrReserve::Done {
      copy_old,
      splitted,
      _guards: guards,
    });
  };

  let mut modified = maybe_owned.is_some();
  let mut leaf = match maybe_owned {
    Some(v) => v,
    None => slot.as_ref().deserialize::<BTreeNode>()?.into_leaf()?,
  };
  while let Some(KeyPair(key, op, create)) =
    others.pop_if(|k| leaf.get_next_key().is_none_or(|r| k < r))
  {
    match try_append_at_leaf_once(
      policy,
      &mut leaf,
      table,
      &key,
      MaybeWritten::Operation(op),
      create,
      &mut slot,
    )? {
      TryAppendAtLeaf::Failed(op) => {
        if modified {
          let Some(prepared) = slot.prepare() else {
            unreachable!();
          };
          policy.serialize_and_log(prepared, &leaf.into_node(), table)?;
        }

        return Ok(AppendOrReserve::Stopped {
          resume: MaybeWrittenKeyPair::Written(key, op, create),
          copy_old,
          splitted,
          _guards: guards,
        });
      }
      TryAppendAtLeaf::NotFound => {}
      TryAppendAtLeaf::Break(g) => {
        guards.push(g);
        modified = true;
      }
      TryAppendAtLeaf::Conflict(i, op) => {
        if modified {
          let Some(prepared) = slot.prepare() else {
            unreachable!();
          };
          policy.serialize_and_log(prepared, &leaf.into_node(), table)?;
        }
        return Ok(AppendOrReserve::Conflict {
          owner: i,
          resume: MaybeWrittenKeyPair::Written(key, op, create),
          copy_old,
          splitted,
          _guards: guards,
        });
      }
      TryAppendAtLeaf::Split(k, p, g) => {
        splitted.push((k, p));
        guards.push(g);
        modified = true;
      }
      TryAppendAtLeaf::CopyOld(cmd) => copy_old.push((key, cmd)),
    };
  }
  if modified {
    let Some(prepared) = slot.prepare() else {
      unreachable!();
    };
    policy.serialize_and_log(prepared, &leaf.into_node(), table)?;
  }
  Ok(AppendOrReserve::Done {
    copy_old,
    splitted,
    _guards: guards,
  })
}

pub fn resolve_conflict<Policy: CreatablePolicy>(policy: &Policy, owner: TxId) -> Result {
  match policy.resolve_conflict(owner) {
    ResolvedConflict::Closed => {
      if policy.is_aborted(owner) {
        return Ok(());
      };
    }
    ResolvedConflict::DeadLock => {}
  }
  Err(Error::WriteConflict)
}
