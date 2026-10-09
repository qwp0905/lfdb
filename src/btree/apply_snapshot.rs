use std::collections::VecDeque;

use crate::{
  blob::{BlobId, BlobLen, BlobOffset},
  cache::{VecRef, WritableSlot},
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
  slot: &mut WritableSlot,
) -> Result<ApplySnapshotOnce> {
  let (pos, found) = match leaf.find_slot(key) {
    FindSlotResult::Move(next) => return Ok(ApplySnapshotOnce::Move(Some(next), record)),
    FindSlotResult::Replace(pos, old, entry_ptr) => {
      if let Some(p) = entry_ptr {
        return Ok(ApplySnapshotOnce::Apply(p, record));
      }

      if !policy.is_aborted(old.owner) {
        if table.is_reserved(key) {
          return Ok(ApplySnapshotOnce::Move(None, record));
        }
        if slot.prepare().is_none() {
          return Ok(ApplySnapshotOnce::Move(None, record));
        }

        let entry_ptr = {
          let Some(slot) = policy.try_alloc_slot(table)? else {
            return Ok(ApplySnapshotOnce::Move(None, record));
          };
          let mut slot = slot.for_write();
          let Some(prepared) = slot.prepare() else {
            table.free().dealloc(slot.get_pointer());
            return Ok(ApplySnapshotOnce::Move(None, record));
          };
          let new_entry = DataEntry::init(record, None);
          policy.serialize_and_log(prepared, &new_entry, table)?;
          prepared.get_pointer()
        };

        leaf.alloc_entry_at(pos, entry_ptr);
        return Ok(ApplySnapshotOnce::Break);
      }

      if slot.prepare().is_none() {
        return Ok(ApplySnapshotOnce::Move(None, record));
      }

      if leaf.is_available(key, &record.data, Some(pos)) {
        leaf.replace_at(pos, record);
        return Ok(ApplySnapshotOnce::Break);
      }

      (pos, true)
    }
    FindSlotResult::Insert(pos) => {
      if slot.prepare().is_none() {
        return Ok(ApplySnapshotOnce::Move(None, record));
      }

      if leaf.is_available(key, &record.data, None) {
        leaf.insert_at(pos, key.to_vec(), record);
        return Ok(ApplySnapshotOnce::Break);
      }
      (pos, false)
    }
  };

  let (mid_key, split_ptr) = {
    let Some(slot) = policy.try_alloc_slot(table)? else {
      return Ok(ApplySnapshotOnce::Move(None, record));
    };
    let mut slot = slot.for_write();
    let Some(prepared) = slot.prepare() else {
      table.free().dealloc(slot.get_pointer());
      return Ok(ApplySnapshotOnce::Move(None, record));
    };

    if found {
      leaf.replace_at(pos, record);
    } else {
      leaf.insert_at(pos, key.to_vec(), record);
    }

    let split = leaf.split_node();
    let mid_key = split.top().to_vec();
    policy.serialize_and_log(prepared, &split.into_node(), table)?;
    (mid_key, prepared.get_pointer())
  };

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
    let Some(slot) = policy.fetch_slot(ptr, table)? else {
      continue 'outer;
    };
    let mut slot = slot.for_write();
    let mut node = slot.as_ref().deserialize::<BTreeNode>()?;
    let leaf = node.as_leaf_mut()?;
    let mut modified = false;
    match apply_snapshot_once(policy, leaf, &key, record, table, &mut slot)? {
      ApplySnapshotOnce::Move(p, r) => {
        (ptr, record) = (p.unwrap_or(ptr), r);
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
      match apply_snapshot_once(policy, leaf, &k, r, table, &mut slot)? {
        ApplySnapshotOnce::Move(p, r) => {
          if modified {
            let Some(prepared) = slot.prepare() else {
              unreachable!();
            };
            policy.serialize_and_log(prepared, &node, table)?;
          }
          (key, ptr, record) = (k, p.unwrap_or(ptr), r);
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
      let Some(prepared) = slot.prepare() else {
        unreachable!();
      };
      policy.serialize_and_log(prepared, &node, table)?;
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
    let Some(slot) = policy.fetch_slot(ptr, table)? else {
      continue;
    };
    let mut slot = slot.for_write();
    let mut entry: DataEntry = slot.as_ref().deserialize()?;
    if entry.is_available(&record) {
      let Some(prepared) = slot.prepare() else {
        continue;
      };
      entry.attach_back(record);
      policy.serialize_and_log(prepared, &entry, table)?;
      return Ok(());
    }

    if let Some(next) = entry.get_next() {
      ptr = next;
      continue;
    };

    let Some(prepared) = slot.prepare() else {
      continue;
    };

    let entry_ptr = {
      let Some(slot) = policy.try_alloc_slot(table)? else {
        continue;
      };
      let mut slot = slot.for_write();
      let Some(prepared) = slot.prepare() else {
        table.free().dealloc(slot.get_pointer());
        continue;
      };

      let new_entry = DataEntry::init(record, None);
      policy.serialize_and_log(prepared, &new_entry, table)?;
      prepared.get_pointer()
    };

    entry.set_next(entry_ptr);
    policy.serialize_and_log(prepared, &entry, table)?;
    return Ok(());
  }
}
