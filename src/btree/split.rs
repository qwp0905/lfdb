use std::mem::replace;

use crate::{
  cache::RefedSlot,
  disk::Pointer,
  objects::{
    BTreeNode, BTreeNodeView, InternalNode, StaticKey, TreeHeader, HEADER_POINTER,
  },
  table::TableHandleRef,
  Result,
};

use super::WritablePolicy;

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
    let state = policy.fetch_slot(ptr, table)?.for_write().mutate(|slot| {
      let mut node = slot.as_ref().deserialize::<BTreeNode>()?;
      let internal = node.as_internal_mut()?;
      if let Err(p) = internal.insert_or_next(&evicted_key, evicted_ptr) {
        return Ok(Err(p));
      };

      let Some((split_node, split_key)) = internal.split_if_needed() else {
        policy.serialize_and_log(slot, &node, table)?;
        return Ok(Ok(None));
      };

      let split_ptr = policy.alloc_and_log(&split_node.into_node(), table)?;
      internal.set_right(&split_key, split_ptr);
      policy.serialize_and_log(slot, &node, table)?;

      Ok(Ok(Some((split_key, split_ptr))))
    })?;
    match state {
      Ok(v) => return Ok(v),
      Err(p) => ptr = p,
    }
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
  slot: &mut RefedSlot,
  table: &TableHandleRef,
  split_key: StaticKey,
  split_pointer: Pointer,
  old_height: usize,
) -> Result<Option<UpdateFailed>> {
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

  let new_root = InternalNode::initialize(split_key, root, split_pointer);
  let new_root_ptr = policy.alloc_and_log(&new_root.into_node(), table)?;

  header.set_root(new_root_ptr);
  header.increase_height();
  policy.serialize_and_log(slot, &header, table)?;
  Ok(None)
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

    let Some(failed) = policy
      .fetch_slot(HEADER_POINTER, table)?
      .for_write()
      .mutate(|slot| {
        update_header(policy, slot, table, split_key, split_pointer, old_height)
      })?
    else {
      return Ok(());
    };

    (split_key, split_pointer) = (failed.split_key, failed.split_pointer);
    old_height += failed.diff;

    let mut ptr = failed.root;
    while stack.len() < failed.diff {
      let slot = policy.fetch_slot(ptr, table)?.for_read();
      let node = slot.as_ref().view::<BTreeNodeView>()?.into_internal()?;
      match node.find(&split_key)? {
        Ok(i) => stack.push(replace(&mut ptr, i)),
        Err(i) => ptr = i,
      }
    }
  }
}
