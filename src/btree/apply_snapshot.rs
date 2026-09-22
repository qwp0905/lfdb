use std::collections::VecDeque;

use crate::{
  blob::{BlobId, BlobLen, BlobOffset},
  cache::VecRef,
  disk::Pointer,
  objects::{
    BTreeNode, DataEntry, FindSlotResult, LeafNode, RecordData, StaticKey, StaticKeyRef,
    VersionRecord,
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
  Move(Pointer, VersionRecord),
  Break,
  Split(StaticKey, Pointer),
  Apply(Pointer, VersionRecord),
}

fn apply_snapshot_once<Policy: WritablePolicy + Sync>(
  policy: &Policy,
  leaf: &mut LeafNode,
  key: StaticKeyRef,
  ptr: Pointer,
  record: VersionRecord,
  table: &TableHandleRef,
) -> Result<ApplySnapshotOnce> {
  match leaf.find_slot(key) {
    FindSlotResult::Move(next) => return Ok(ApplySnapshotOnce::Move(next, record)),
    FindSlotResult::Replace(pos, old, entry_ptr) => {
      if let Some(p) = entry_ptr {
        return Ok(ApplySnapshotOnce::Apply(p, record));
      }

      if !policy.is_aborted(old.owner) {
        if table.is_reserved(key) {
          return Ok(ApplySnapshotOnce::Move(ptr, record));
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

fn into_record(snapshot: KVSnapshot) -> (VecRef, VersionRecord) {
  let key = snapshot.key;
  let data = match snapshot.value {
    SnapshotValue::Data(data) => RecordData::Data(data.to_vec()),
    SnapshotValue::Blob(id, offset, len) => RecordData::Blob(id, offset, len),
  };
  let record = VersionRecord::new(snapshot.owner, snapshot.version, data);
  (key, record)
}

pub fn drain_snapshot_once<Policy: WritablePolicy + Sync>(
  policy: &Policy,
  must_apply: KVSnapshot,
  table: &TableHandleRef,
  leaf_ptr: Pointer,
  stack: Vec<Pointer>,
  bulk: &mut VecDeque<KVSnapshot>,
) -> Result {
  let (mut key, mut record) = into_record(must_apply);
  let mut splitted = Vec::new();
  let mut apply = Vec::new();
  let mut ptr = leaf_ptr;

  'outer: loop {
    let mut slot = policy.fetch_slot(ptr, table)?.for_write();
    let mut node = slot.as_ref().deserialize::<BTreeNode>()?;
    let leaf = node.as_leaf_mut()?;
    let mut modified = false;
    match apply_snapshot_once(policy, leaf, &key, ptr, record, table)? {
      ApplySnapshotOnce::Move(p, r) => {
        (ptr, record) = (p, r);
        continue 'outer;
      }
      ApplySnapshotOnce::Break => modified = true,
      ApplySnapshotOnce::Split(k, p) => {
        splitted.push((k, p));
        modified = true;
      }
      ApplySnapshotOnce::Apply(p, record) => apply.push((p, record)),
    };

    while let Some(snapshot) =
      bulk.pop_front_if(|s| leaf.get_next_key().is_none_or(|r| r > &*s.key))
    {
      let (k, r) = into_record(snapshot);
      match apply_snapshot_once(policy, leaf, &k, ptr, r, table)? {
        ApplySnapshotOnce::Move(p, r) => {
          if modified {
            policy.serialize_and_log(&mut slot, &node, table)?;
          }
          (key, ptr, record) = (k, p, r);
          continue 'outer;
        }
        ApplySnapshotOnce::Break => modified = true,
        ApplySnapshotOnce::Split(k, p) => {
          splitted.push((k, p));
          modified = true;
        }
        ApplySnapshotOnce::Apply(p, record) => apply.push((p, record)),
      };
    }
    if modified {
      policy.serialize_and_log(&mut slot, &node, table)?;
    }
    break 'outer;
  }

  for (entry_ptr, record) in apply {
    apply_snapshot_at_entry(policy, entry_ptr, record, table)?;
  }
  for (k, p) in splitted {
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
  record: VersionRecord,
  table: &TableHandleRef,
) -> Result {
  let mut ptr = entry_ptr;
  loop {
    let mut slot = policy.fetch_slot(ptr, table)?.for_write();
    let mut entry: DataEntry = slot.as_ref().deserialize()?;
    if entry.is_available(&record) {
      entry.attach_back(record);
      policy.serialize_and_log(&mut slot, &entry, table)?;
      return Ok(());
    }

    if let Some(next) = entry.get_next() {
      ptr = next;
      continue;
    }

    let new_entry = DataEntry::init(record, None);
    entry.set_next(policy.alloc_and_log(&new_entry, table)?);
    policy.serialize_and_log(&mut slot, &entry, table)?;
    return Ok(());
  }
}
