use std::mem::replace;

use crate::{
  disk::Pointer,
  objects::{
    BTreeNode, BTreeNodeView, InternalNode, StaticKey, StaticKeyRef, TreeHeader,
    HEADER_POINTER,
  },
  table::TableHandleRef,
  Result,
};

use super::{read_header, ReadonlyPolicy, WritablePolicy};

/**
 * Apply one propagated split to an internal level. The stack entry is only an
 * anchor; the function follows B-link right moves until it reaches the node that
 * should receive the separator. If that node splits, return the next separator
 * for the parent level. Otherwise propagation stops.
 */
fn apply_split<Policy: WritablePolicy + Sync>(
  policy: &Policy,
  evicted_key: StaticKey,
  evicted_ptr: Pointer,
  current: Pointer,
  table: &TableHandleRef,
) -> Result<Option<(StaticKey, Pointer)>> {
  let mut ptr = current;
  loop {
    let Some(slot) = policy.fetch_slot(ptr, table)? else {
      continue;
    };
    let mut slot = slot.for_write();
    let mut node = slot.as_ref().deserialize::<BTreeNode>()?;
    let internal = node.as_internal_mut()?;
    if let Err(p) = internal.insert_or_next(&evicted_key, evicted_ptr) {
      ptr = p;
      continue;
    };

    let Some(prepared) = slot.prepare() else {
      continue;
    };

    let Some((split_node, split_key)) = internal.split_if_needed() else {
      policy.serialize_and_log(prepared, &node, table)?;
      return Ok(None);
    };

    let Some(split_ptr) = policy.alloc_and_log(&split_node.into_node(), table)? else {
      continue;
    };
    internal.set_right(&split_key, split_ptr);
    policy.serialize_and_log(prepared, &node, table)?;
    return Ok(Some((split_key, split_ptr)));
  }
}

struct UpdateFailed {
  root: Pointer,
  diff: usize,
  split_key: StaticKey,
  split_pointer: Pointer,
}

fn update_header<Policy: WritablePolicy + Sync>(
  policy: &Policy,
  table: &TableHandleRef,
  split_key: StaticKey,
  split_pointer: Pointer,
  old_height: usize,
) -> Result<Option<UpdateFailed>> {
  loop {
    let Some(slot) = policy.fetch_slot(HEADER_POINTER, table)? else {
      continue;
    };

    let mut slot = slot.for_write();
    let mut header: TreeHeader = slot.as_ref().deserialize()?;
    let current_height = header.get_height() as usize;
    let root = header.get_root();
    if old_height != current_height {
      return Ok(Some(UpdateFailed {
        root,
        diff: current_height - old_height,
        split_key,
        split_pointer,
      }));
    }

    let Some(prepared) = slot.prepare() else {
      continue;
    };

    let new_root_ptr = {
      let Some(new_slot) = policy.try_alloc_slot(table)? else {
        continue;
      };
      let mut new_slot = new_slot.for_write();
      let Some(new_prepared) = new_slot.prepare() else {
        table.free().dealloc(new_slot.get_pointer());
        continue;
      };

      let new_root = InternalNode::initialize(split_key, root, split_pointer);
      let node = new_root.into_node();
      policy.serialize_and_log(new_prepared, &node, table)?;
      new_prepared.get_pointer()
    };

    header.set_root(new_root_ptr);
    header.increase_height();
    policy.serialize_and_log(prepared, &header, table)?;
    return Ok(None);
  }
}

pub fn propagate_split<Policy: WritablePolicy + Sync>(
  policy: &Policy,
  mut split_key: StaticKey,
  mut split_pointer: Pointer,
  mut stack: Vec<Pointer>,
  table: &TableHandleRef,
  height: usize,
) -> Result {
  let mut old_height = height;
  loop {
    while let Some(ptr) = stack.pop() {
      match apply_split(policy, split_key, split_pointer, ptr, table)? {
        Some((k, p)) => (split_key, split_pointer) = (k, p),
        None => return Ok(()),
      }
    }

    let Some(failed) =
      update_header(policy, table, split_key, split_pointer, old_height)?
    else {
      return Ok(());
    };

    (split_key, split_pointer) = (failed.split_key, failed.split_pointer);
    old_height += failed.diff;

    let mut ptr = failed.root;
    while stack.len() < failed.diff {
      match find_key_from_internal(policy, &split_key, ptr, table)? {
        Ok(i) => stack.push(replace(&mut ptr, i)),
        Err(i) => ptr = i,
      }
    }
  }
}

fn find_key_from_internal<Policy: ReadonlyPolicy + Sync>(
  policy: &Policy,
  split_key: StaticKeyRef,
  ptr: Pointer,
  table: &TableHandleRef,
) -> Result<std::result::Result<Pointer, Pointer>> {
  loop {
    if let Some(slot) = policy.fetch_slot(ptr, table)? {
      let slot = slot.for_read();
      let node = slot.as_ref().view::<BTreeNodeView>()?.into_internal()?;
      return node.find(split_key);
    }
  }
}

pub fn recovery_half_split<Policy: WritablePolicy + Sync>(
  policy: &Policy,
  split_key: StaticKey,
  split_pointer: Pointer,
  level: u16,
  table: &TableHandleRef,
) -> Result {
  let (mut ptr, height) = {
    let header = read_header(policy, table)?;
    (header.get_root(), header.get_height() as usize)
  };

  let diff = height - level as usize;
  let mut stack = vec![];
  while stack.len() < diff {
    match find_key_from_internal(policy, &split_key, ptr, table)? {
      Ok(i) => stack.push(replace(&mut ptr, i)),
      Err(i) => ptr = i,
    }
  }

  propagate_split(policy, split_key, split_pointer, stack, table, height)
}
