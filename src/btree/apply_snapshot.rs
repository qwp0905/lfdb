use std::collections::VecDeque;

use crate::{
  blob::{BlobAppendGuard, BlobId, BlobLen, BlobOffset},
  disk::Pointer,
  objects::{
    BTreeNode, DataEntry, FindSlotResult, LeafNode, RecordData, StaticKey, StaticKeyRef,
    VersionRecord, LARGE_VALUE,
  },
  page::VecRef,
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

        let entry_ptr = policy.commit_alloc(&DataEntry::init(record, None), table)?;
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
  let split_ptr = policy.commit_alloc(&split.into_node(), table)?;

  leaf.set_next(mid_key.clone(), split_ptr);
  Ok(ApplySnapshotOnce::Split(mid_key, split_ptr))
}

fn into_record<'a, Policy: WritablePolicy>(
  policy: &'a Policy,
  snapshot: KVSnapshot,
) -> Result<(VecRef, VersionRecord, Option<BlobAppendGuard<'a>>)> {
  let key = snapshot.key;
  let (data, guard) = match snapshot.value {
    SnapshotValue::Data(data) if data.len() > LARGE_VALUE => {
      let guard = policy.write_blob(data.to_vec())?;
      let data = RecordData::Blob(guard.get_id(), guard.get_offset(), guard.get_len());
      (data, Some(guard))
    }
    SnapshotValue::Data(data) => (RecordData::Data(data.to_vec()), None),
    SnapshotValue::Blob(id, offset, len) if len as usize <= LARGE_VALUE => {
      let data = policy.read_blob(id, offset, len)?;
      (RecordData::Data(data.as_slice().to_vec()), None)
    }
    SnapshotValue::Blob(id, offset, len) => (RecordData::Blob(id, offset, len), None),
  };
  let record = VersionRecord::new(snapshot.owner, snapshot.version, data);
  Ok((key, record, guard))
}

pub fn drain_snapshot_once<Policy: WritablePolicy + Sync>(
  policy: &Policy,
  must_apply: KVSnapshot,
  table: &TableHandleRef,
  leaf_ptr: Pointer,
  stack: Vec<Pointer>,
  bulk: &mut VecDeque<KVSnapshot>,
) -> Result {
  let mut guards = Vec::new();
  let (mut key, mut record, guard) = into_record(policy, must_apply)?;
  guards.extend(guard);
  let mut splitted = Vec::new();
  let mut apply = Vec::new();
  let mut ptr = leaf_ptr;

  'outer: loop {
    let slot = policy.fetch_slot(ptr, table)?.for_write();
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
      let (k, r, guard) = into_record(policy, snapshot)?;
      guards.extend(guard);
      match apply_snapshot_once(policy, leaf, &k, ptr, r, table)? {
        ApplySnapshotOnce::Move(p, r) => {
          if modified {
            policy.commit_write(slot, &node, table)?;
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
      policy.commit_write(slot, &node, table)?;
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
    let slot = policy.fetch_slot(ptr, table)?.for_write();
    let mut entry: DataEntry = slot.as_ref().deserialize()?;
    if entry.is_available(&record) {
      entry.attach_back(record);
      policy.commit_write(slot, &entry, table)?;
      return Ok(());
    }

    if let Some(next) = entry.get_next() {
      ptr = next;
      continue;
    }

    let new_entry = DataEntry::init(record, None);
    entry.set_next(policy.commit_alloc(&new_entry, table)?);
    policy.commit_write(slot, &entry, table)?;
    return Ok(());
  }
}
