use std::{
  collections::{BTreeSet, VecDeque},
  iter::Peekable,
};

use crate::{
  blob::BlobAppendGuard,
  cache::RefedSlot,
  disk::Pointer,
  objects::{
    BTreeNode, BTreeNodeView, DataEntry, FindSlotResult, LeafNode, LeafNodeView,
    NodeFindResult, RecordData, StaticKey, StaticKeyRef, VersionRecord,
    VersionRecordView, LARGE_VALUE,
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
  policy
    .fetch_slot(entry_ptr, table)?
    .for_write()
    .mutate(|slot| {
      let mut entry = slot.as_ref().deserialize::<DataEntry>()?;
      if entry.is_available(&old) {
        entry.attach_front(old);
        policy.serialize_and_log(slot, &entry, table)?;
        return Ok(());
      }

      let new_entry_ptr = policy.alloc_and_log(&entry, table)?;
      let new_entry = DataEntry::init(old, Some(new_entry_ptr));
      policy.serialize_and_log(slot, &new_entry, table)?;
      Ok(())
    })
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
  Move(Pointer, WriteOp),
  Break(MaybeBlobGuard<'a>),
  Split(StaticKey, Pointer, MaybeBlobGuard<'a>),
}
fn complete_leaf_once<'a, Policy: CreatablePolicy + Sync>(
  policy: &'a Policy,
  leaf: &mut LeafNode,
  key: StaticKeyRef,
  operation: WriteOp,
  entry_ptr: Pointer,
  table: &TableHandleRef,
) -> Result<CompleteLeafOnce<'a>> {
  let pos = match leaf.find_slot(key) {
    FindSlotResult::Replace(i, _, _) => i,
    FindSlotResult::Move(next) => return Ok(CompleteLeafOnce::Move(next, operation)),
    FindSlotResult::Insert(_) => unreachable!(),
  };

  let (record, guard) = create_record(policy, operation)?;

  let new_record =
    VersionRecord::new(policy.current_owner(), policy.current_version(), record);
  leaf.replace_at(pos, new_record);
  leaf.alloc_entry_at(pos, entry_ptr);

  let Some(split) = leaf.split_if_needed() else {
    return Ok(CompleteLeafOnce::Break(guard));
  };

  let mid_key = split.top().clone();
  let split_ptr = policy.alloc_and_log(&split.into_node(), table)?;

  leaf.set_next(mid_key.clone(), split_ptr);
  Ok(CompleteLeafOnce::Split(mid_key, split_ptr, guard))
}

struct LeafCompletion<'a> {
  insert_guard: ReserveGuard<'a>,
  entry_ptr: Pointer,
  key: StaticKey,
  operation: WriteOp,
}
impl<'a> LeafCompletion<'a> {
  const fn new(
    insert_guard: ReserveGuard<'a>,
    entry_ptr: Pointer,
    key: StaticKey,
    operation: WriteOp,
  ) -> Self {
    Self {
      insert_guard,
      entry_ptr,
      key,
      operation,
    }
  }
}

fn complete_leaf<Policy: CreatablePolicy + Sync>(
  policy: &Policy,
  mut leaf_ptr: Pointer,
  table: &TableHandleRef,
  must_apply: LeafCompletion,
  buffered: &mut VecDeque<LeafCompletion>,
) -> Result<(Vec<(StaticKey, Pointer)>, Pointer)> {
  enum State<'a> {
    Break,
    Move(Pointer, ReserveGuard<'a>, WriteOp),
  }

  let mut splitted = Vec::new();
  let LeafCompletion {
    mut insert_guard,
    entry_ptr,
    key,
    mut operation,
  } = must_apply;

  loop {
    let mut guards = Vec::new();
    let state = policy
      .fetch_slot(leaf_ptr, table)?
      .for_write()
      .mutate(|slot| {
        let mut node = slot.as_ref().deserialize::<BTreeNode>()?;
        let leaf = node.as_leaf_mut()?;
        match complete_leaf_once(policy, leaf, &key, operation, entry_ptr, table)? {
          CompleteLeafOnce::Move(p, op) => return Ok(State::Move(p, insert_guard, op)),
          CompleteLeafOnce::Break(guard) => guards.push((insert_guard, guard)),
          CompleteLeafOnce::Split(k, p, guard) => {
            guards.push((insert_guard, guard));
            splitted.push((k, p));
          }
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
          match complete_leaf_once(policy, leaf, &key, operation, entry_ptr, table)? {
            CompleteLeafOnce::Move(_, _) => unreachable!(),
            CompleteLeafOnce::Break(guard) => guards.push((insert_guard, guard)),
            CompleteLeafOnce::Split(k, p, guard) => {
              guards.push((insert_guard, guard));
              splitted.push((k, p));
            }
          };
        }
        policy.serialize_and_log(slot, &node, table)?;
        Ok(State::Break)
      })?;
    match state {
      State::Break => return Ok((splitted, leaf_ptr)),
      State::Move(p, ig, op) => (leaf_ptr, insert_guard, operation) = (p, ig, op),
    }
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
      None => policy.alloc_and_log(&DataEntry::init(old, None), table)?,
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
  pub operation: WriteOp,
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

enum TryReserveOrFindPos<'a> {
  Move(Pointer),
  Conflict(TxId),
  CopyOld(ReserveGuard<'a>, Option<Pointer>, VersionRecordView),
  PosFound(usize, bool),
}
fn try_reserve_or_find_pos<'a, Policy: CreatablePolicy>(
  policy: &Policy,
  leaf: &LeafNodeView,
  table: &'a TableHandleRef,
  key: StaticKeyRef,
) -> Result<TryReserveOrFindPos<'a>> {
  let result = match leaf.find(key)? {
    NodeFindResult::Move(p) => TryReserveOrFindPos::Move(p),
    NodeFindResult::Found(pos, old, entry_ptr) => {
      let writable = policy.is_owned(old.owner) || policy.is_aborted(old.owner);
      let visible = policy.is_readable(old.version) && !policy.is_active(old.owner);
      match (writable, visible) {
        (true, _) => TryReserveOrFindPos::PosFound(pos, true),
        (false, false) => TryReserveOrFindPos::Conflict(old.owner),
        (false, true) => match table.reserve(key.to_vec(), policy.current_owner()) {
          Ok(g) => TryReserveOrFindPos::CopyOld(g, entry_ptr, old),
          Err(i) => TryReserveOrFindPos::Conflict(i),
        },
      }
    }
    NodeFindResult::NotFound(pos) => TryReserveOrFindPos::PosFound(pos, false),
  };
  Ok(result)
}

type MaybeBlobGuard<'a> = Option<BlobAppendGuard<'a>>;

fn apply_operation<'a, Policy: CreatablePolicy + Sync>(
  policy: &'a Policy,
  node: &mut LeafNode,
  key: StaticKeyRef,
  op: WriteOp,
  table: &TableHandleRef,
  pos: usize,
  found: bool,
) -> Result<(Option<(StaticKey, Pointer)>, MaybeBlobGuard<'a>)> {
  let (record, guard) = create_record(policy, op)?;
  let new_record =
    VersionRecord::new(policy.current_owner(), policy.current_version(), record);
  if found {
    node.replace_at(pos, new_record);
  } else {
    node.insert_at(pos, key.to_vec(), new_record);
  };

  let Some(split) = node.split_if_needed() else {
    return Ok((None, guard));
  };

  let mid_key = split.top().clone();
  let split_ptr = policy.alloc_and_log(&split.into_node(), table)?;

  node.set_next(mid_key.clone(), split_ptr);
  Ok((Some((mid_key, split_ptr)), guard))
}

enum TryAppendAtLeaf<'a> {
  DoNothing,
  Break(MaybeBlobGuard<'a>),
  Conflict(TxId, WriteOp),
  Split(StaticKey, Pointer, MaybeBlobGuard<'a>),
  CopyOld(CopyOld<'a>),
}
fn try_append_at_leaf_once<'a, Policy: CreatablePolicy + Sync>(
  policy: &'a Policy,
  leaf: &mut LeafNode,
  table: &'a TableHandleRef,
  key: StaticKeyRef,
  op: WriteOp,
  create: bool,
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
        return Ok(TryAppendAtLeaf::DoNothing);
      }
      (pos, false, op)
    }
  };

  match apply_operation(policy, leaf, key, op, table, pos, found)? {
    (None, g) => Ok(TryAppendAtLeaf::Break(g)),
    (Some((k, p)), g) => Ok(TryAppendAtLeaf::Split(k, p, g)),
  }
}

pub enum AppendOrReserve<'a> {
  Move(Pointer, KeyPair),
  Conflict {
    owner: TxId,
    resume: KeyPair,
    copy_old: Vec<(StaticKey, CopyOld<'a>)>,
    splitted: Vec<(StaticKey, Pointer)>,
    _guards: Vec<MaybeBlobGuard<'a>>,
  },
  Done {
    copy_old: Vec<(StaticKey, CopyOld<'a>)>,
    splitted: Vec<(StaticKey, Pointer)>,
    _guards: Vec<MaybeBlobGuard<'a>>,
  },
}

fn append_with_owned_leaf<'a, Policy: CreatablePolicy + Sync>(
  policy: &'a Policy,
  slot: &mut RefedSlot,
  mut leaf: LeafNode,
  table: &'a TableHandleRef,
  others: &mut KeyPairList,
  mut splitted: Vec<(StaticKey, Pointer)>,
  mut copy_old: Vec<(StaticKey, CopyOld<'a>)>,
  mut guards: Vec<MaybeBlobGuard<'a>>,
) -> Result<AppendOrReserve<'a>> {
  while let Some(KeyPair(key, op, create)) =
    others.pop_if(|k| leaf.get_next_key().is_none_or(|r| k < r))
  {
    match try_append_at_leaf_once(policy, &mut leaf, table, &key, op, create)? {
      TryAppendAtLeaf::DoNothing => {}
      TryAppendAtLeaf::Break(g) => guards.push(g),
      TryAppendAtLeaf::Conflict(i, op) => {
        policy.serialize_and_log(slot, &leaf.into_node(), table)?;
        return Ok(AppendOrReserve::Conflict {
          owner: i,
          resume: KeyPair(key, op, create),
          copy_old,
          splitted,
          _guards: guards,
        });
      }
      TryAppendAtLeaf::Split(k, p, g) => {
        splitted.push((k, p));
        guards.push(g);
      }
      TryAppendAtLeaf::CopyOld(cmd) => copy_old.push((key, cmd)),
    }
  }
  policy.serialize_and_log(slot, &leaf.into_node(), table)?;
  Ok(AppendOrReserve::Done {
    copy_old,
    splitted,
    _guards: guards,
  })
}

pub fn append_or_reserve_at_leaf<'a, Policy: CreatablePolicy + Sync>(
  policy: &'a Policy,
  slot: &mut RefedSlot,
  must_apply: KeyPair,
  table: &'a TableHandleRef,
  others: Option<&mut KeyPairList>,
) -> Result<AppendOrReserve<'a>> {
  let mut splitted = Vec::new();
  let mut copy_old = Vec::new();
  let mut guards = Vec::new();

  let leaf = slot.as_ref().view::<BTreeNodeView>()?.into_leaf()?;
  let KeyPair(key, op, create) = must_apply;
  match try_reserve_or_find_pos(policy, &leaf, table, &key)? {
    TryReserveOrFindPos::Move(p) => {
      return Ok(AppendOrReserve::Move(p, KeyPair(key, op, create)))
    }
    TryReserveOrFindPos::Conflict(i) => {
      return Ok(AppendOrReserve::Conflict {
        owner: i,
        resume: KeyPair(key, op, create),
        copy_old,
        splitted,
        _guards: guards,
      })
    }
    TryReserveOrFindPos::CopyOld(g, entry_ptr, old) => {
      let cmd = CopyOld::new(g, entry_ptr, old.into_owned_with(slot.as_ref()), op);
      copy_old.push((key, cmd));
    }
    TryReserveOrFindPos::PosFound(pos, found) => {
      if create || found {
        let mut leaf = leaf.into_owned()?;
        let (s, guard) = apply_operation(policy, &mut leaf, &key, op, table, pos, found)?;
        if let Some(s) = s {
          splitted.push(s);
        }
        guards.push(guard);

        let Some(others) = others else {
          policy.serialize_and_log(slot, &leaf.into_node(), table)?;
          return Ok(AppendOrReserve::Done {
            copy_old,
            splitted,
            _guards: guards,
          });
        };

        return append_with_owned_leaf(
          policy, slot, leaf, table, others, splitted, copy_old, guards,
        );
      }
    }
  };

  let Some(others) = others else {
    return Ok(AppendOrReserve::Done {
      copy_old,
      splitted,
      _guards: guards,
    });
  };

  while let Some(KeyPair(key, op, create)) =
    others.pop_if(|k| leaf.get_next_key().is_none_or(|r| k < r))
  {
    match try_reserve_or_find_pos(policy, &leaf, table, &key)? {
      TryReserveOrFindPos::Move(_) => unreachable!(),
      TryReserveOrFindPos::Conflict(i) => {
        return Ok(AppendOrReserve::Conflict {
          owner: i,
          resume: KeyPair(key, op, create),
          copy_old,
          splitted,
          _guards: guards,
        })
      }
      TryReserveOrFindPos::CopyOld(g, entry_ptr, old) => {
        let cmd = CopyOld::new(g, entry_ptr, old.into_owned_with(slot.as_ref()), op);
        copy_old.push((key, cmd));
      }
      TryReserveOrFindPos::PosFound(pos, found) => {
        if !create && !found {
          continue;
        }

        let mut leaf = leaf.into_owned()?;
        let (s, guard) = apply_operation(policy, &mut leaf, &key, op, table, pos, found)?;
        if let Some(s) = s {
          splitted.push(s);
        }
        guards.push(guard);
        return append_with_owned_leaf(
          policy, slot, leaf, table, others, splitted, copy_old, guards,
        );
      }
    }
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
