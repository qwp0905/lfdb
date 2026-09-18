use std::collections::VecDeque;

use crate::{
  blob::{BlobId, BlobLen, BlobOffset},
  cache::{RefedSlot, VecRef},
  disk::Pointer,
  objects::{
    BTreeNodeView, DataEntry, FindSlotResult, LeafNode, LeafNodeView, NodeFindResult,
    RecordData, StaticKey, StaticKeyRef, VersionRecord,
  },
  table::TableHandleRef,
  wal::TxId,
  Result,
};

use super::{propagate_split, WritablePolicy};

/**
 * Buffered record payload used by snapshot-oriented iteration.
 *
 * Inline data is kept as bytes, while blob data keeps its existing blob pointer.
 * Blob segments are reclaimed by reference counting and their locations are
 * stable, so snapshot/compaction does not copy blob bytes through this iterator.
 */
pub enum SnapshotValue {
  Data(VecRef),
  Blob(BlobId, BlobOffset, BlobLen),
}

pub struct KVSnapshot {
  pub key: VecRef,
  pub value: SnapshotValue,
  pub owner: TxId,
  pub version: TxId,
}

enum ApplySnapshotOnce {
  Move(Option<Pointer>, VersionRecord),
  Break,
  Split(StaticKey, Pointer),
  Apply(Pointer, VersionRecord),
}

fn apply_snapshot_once<Policy: WritablePolicy + Sync>(
  policy: &Policy,
  leaf: &mut LeafNode,
  key: StaticKeyRef,
  record: VersionRecord,
  table: &TableHandleRef,
) -> Result<ApplySnapshotOnce> {
  match leaf.find_slot(key) {
    FindSlotResult::Move(next) => return Ok(ApplySnapshotOnce::Move(Some(next), record)),
    FindSlotResult::Replace(pos, old, entry_ptr) => {
      if let Some(p) = entry_ptr {
        return Ok(ApplySnapshotOnce::Apply(p, record));
      }

      if !policy.is_aborted(old.owner) {
        if table.is_reserved(key) {
          return Ok(ApplySnapshotOnce::Move(None, record));
        }

        let entry_ptr = policy.alloc_and_log(&DataEntry::init(record, None), table)?;
        leaf.alloc_entry_at(pos, entry_ptr);
        return Ok(ApplySnapshotOnce::Break);
      }

      leaf.replace_at(pos, record);
    }
    FindSlotResult::Insert(pos) => {
      leaf.insert_at(pos, key.to_vec(), record);
    }
  };

  let Some(split) = leaf.split_if_needed() else {
    return Ok(ApplySnapshotOnce::Break);
  };

  let mid_key = split.top().clone();
  let split_ptr = policy.alloc_and_log(&split.into_node(), table)?;

  leaf.set_next(mid_key.clone(), split_ptr);
  Ok(ApplySnapshotOnce::Split(mid_key, split_ptr))
}

enum ApplySnapshotBorrowed<'a> {
  Move(Option<Pointer>, VersionRecord),
  Break(LeafNode),
  Split(StaticKey, Pointer, LeafNode),
  Apply(Pointer, VersionRecord, LeafNodeView<'a>),
}

fn apply_snapshot_borrowed<'a, Policy: WritablePolicy + Sync>(
  policy: &Policy,
  leaf: LeafNodeView<'a>,
  key: StaticKeyRef,
  record: VersionRecord,
  table: &TableHandleRef,
) -> Result<ApplySnapshotBorrowed<'a>> {
  let mut leaf = match leaf.find(key)? {
    NodeFindResult::Move(next) => {
      return Ok(ApplySnapshotBorrowed::Move(Some(next), record))
    }
    NodeFindResult::Found(pos, old, entry_ptr) => {
      if let Some(p) = entry_ptr {
        return Ok(ApplySnapshotBorrowed::Apply(p, record, leaf));
      }

      if !policy.is_aborted(old.owner) {
        if table.is_reserved(key) {
          return Ok(ApplySnapshotBorrowed::Move(None, record));
        }

        let mut leaf = leaf.into_owned()?;
        let entry_ptr = policy.alloc_and_log(&DataEntry::init(record, None), table)?;
        leaf.alloc_entry_at(pos, entry_ptr);
        return Ok(ApplySnapshotBorrowed::Break(leaf));
      }

      let mut leaf = leaf.into_owned()?;
      leaf.replace_at(pos, record);
      leaf
    }
    NodeFindResult::NotFound(pos) => {
      let mut leaf = leaf.into_owned()?;
      leaf.insert_at(pos, key.to_vec(), record);
      leaf
    }
  };

  let Some(split) = leaf.split_if_needed() else {
    return Ok(ApplySnapshotBorrowed::Break(leaf));
  };

  let mid_key = split.top().clone();
  let split_ptr = policy.alloc_and_log(&split.into_node(), table)?;

  leaf.set_next(mid_key.clone(), split_ptr);
  Ok(ApplySnapshotBorrowed::Split(mid_key, split_ptr, leaf))
}

fn into_record(snapshot: KVSnapshot) -> (VecRef, VersionRecord) {
  let key = snapshot.key;
  let data = match snapshot.value {
    SnapshotValue::Data(data) => RecordData::Data(data.to_vec()),
    SnapshotValue::Blob(id, offset, len) => RecordData::Blob(id, offset, len),
  };
  let record = VersionRecord::new(snapshot.owner, snapshot.version, data);
  (key, record)
}

type DrainSnapshotOwnedErr = (VecRef, Option<Pointer>, VersionRecord);
fn drain_snapshot_owned<Policy: WritablePolicy + Sync>(
  policy: &Policy,
  slot: &mut RefedSlot,
  mut leaf: LeafNode,
  table: &TableHandleRef,
  bulk: &mut VecDeque<KVSnapshot>,
  state: &mut DrainSnapshotState,
) -> Result<std::result::Result<(), DrainSnapshotOwnedErr>> {
  while let Some(snapshot) =
    bulk.pop_front_if(|s| leaf.get_next_key().is_none_or(|r| r > &*s.key))
  {
    let (key, record) = into_record(snapshot);
    match apply_snapshot_once(policy, &mut leaf, &key, record, table)? {
      ApplySnapshotOnce::Move(p, r) => {
        policy.serialize_and_log(slot, &leaf.into_node(), table)?;
        return Ok(Err((key, p, r)));
      }
      ApplySnapshotOnce::Break => {}
      ApplySnapshotOnce::Split(k, p) => {
        state.splitted.get_or_insert_default().push((k, p));
      }
      ApplySnapshotOnce::Apply(p, record) => {
        state.apply.get_or_insert_default().push((p, record))
      }
    };
  }
  policy.serialize_and_log(slot, &leaf.into_node(), table)?;
  Ok(Ok(()))
}

struct DrainSnapshotState {
  apply: Option<Vec<(Pointer, VersionRecord)>>,
  splitted: Option<Vec<(StaticKey, Pointer)>>,
}
impl DrainSnapshotState {
  const fn new() -> Self {
    Self {
      apply: None,
      splitted: None,
    }
  }
}

pub fn drain_snapshot_once<Policy: WritablePolicy + Sync>(
  policy: &Policy,
  must_apply: KVSnapshot,
  table: &TableHandleRef,
  leaf_ptr: Pointer,
  stack: Vec<Pointer>,
  bulk: &mut VecDeque<KVSnapshot>,
) -> Result {
  enum MaybeOwned<'a> {
    Owned(LeafNode),
    Borrowed(LeafNodeView<'a>),
  }

  let (mut key, mut record) = into_record(must_apply);
  let mut state = DrainSnapshotState::new();
  let mut ptr = leaf_ptr;

  loop {
    let result = policy.fetch_slot(ptr, table)?.for_write().mutate(|slot| {
      let leaf = slot.as_ref().view::<BTreeNodeView>()?.into_leaf()?;
      let maybe_owned = match apply_snapshot_borrowed(policy, leaf, &key, record, table)?
      {
        ApplySnapshotBorrowed::Move(p, r) => return Ok(Err((key, p, r))),
        ApplySnapshotBorrowed::Break(l) => MaybeOwned::Owned(l),
        ApplySnapshotBorrowed::Split(k, p, l) => {
          state.splitted.get_or_insert_default().push((k, p));
          MaybeOwned::Owned(l)
        }
        ApplySnapshotBorrowed::Apply(p, r, l) => {
          state.apply.get_or_insert_default().push((p, r));
          MaybeOwned::Borrowed(l)
        }
      };

      let mut leaf = match maybe_owned {
        MaybeOwned::Owned(leaf) => {
          return drain_snapshot_owned(policy, slot, leaf, table, bulk, &mut state)
        }
        MaybeOwned::Borrowed(leaf) => leaf,
      };

      while let Some(snapshot) =
        bulk.pop_front_if(|s| leaf.get_next_key().is_none_or(|r| r > &*s.key))
      {
        let (key, record) = into_record(snapshot);
        let owned = match apply_snapshot_borrowed(policy, leaf, &key, record, table)? {
          ApplySnapshotBorrowed::Move(p, r) => return Ok(Err((key, p, r))),
          ApplySnapshotBorrowed::Break(owned) => owned,
          ApplySnapshotBorrowed::Split(k, p, owned) => {
            state.splitted.get_or_insert_default().push((k, p));
            owned
          }
          ApplySnapshotBorrowed::Apply(p, r, l) => {
            state.apply.get_or_insert_default().push((p, r));
            leaf = l;
            continue;
          }
        };
        return drain_snapshot_owned(policy, slot, owned, table, bulk, &mut state);
      }
      Ok(Ok(()))
    })?;
    match result {
      Ok(_) => break,
      Err((k, p, r)) => (key, record, ptr) = (k, r, p.unwrap_or(ptr)),
    }
  }

  for (entry_ptr, record) in state.apply.into_iter().flatten() {
    apply_snapshot_at_entry(policy, entry_ptr, record, table)?;
  }
  for (k, p) in state.splitted.into_iter().flatten() {
    propagate_split(policy, k, p, stack.clone(), table, stack.len())?;
  }
  Ok(())
}

/**
 * Append a snapshot version to the end of an existing data-entry chain.
 *
 * The caller guarantees ordering: the supplied record belongs after the records
 * already present for this key.
 */
fn apply_snapshot_at_entry<Policy: WritablePolicy + Sync>(
  policy: &Policy,
  entry_ptr: Pointer,
  mut record: VersionRecord,
  table: &TableHandleRef,
) -> Result {
  let mut ptr = entry_ptr;
  loop {
    let state = policy.fetch_slot(ptr, table)?.for_write().mutate(|slot| {
      let mut entry: DataEntry = slot.as_ref().deserialize()?;
      if entry.is_available(&record) {
        entry.attach_back(record);
        policy.serialize_and_log(slot, &entry, table)?;
        return Ok(Ok(()));
      }

      if let Some(next) = entry.get_next() {
        return Ok(Err((next, record)));
      }

      let new_entry = DataEntry::init(record, None);
      entry.set_next(policy.alloc_and_log(&new_entry, table)?);
      policy.serialize_and_log(slot, &entry, table)?;
      Ok(Ok(()))
    })?;
    match state {
      Ok(_) => return Ok(()),
      Err((i, r)) => (ptr, record) = (i, r),
    }
  }
}
