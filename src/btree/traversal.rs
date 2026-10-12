use crate::{
  cache::ReadonlySlot,
  disk::Pointer,
  objects::{
    BTreeNodeView, DataEntryView, NodeFindResult, StaticKeyRef, TreeHeader,
    VersionRecordView, HEADER_POINTER,
  },
  table::TableHandleRef,
  Result,
};

use super::ReadonlyPolicy;

pub fn read_header<Policy: ReadonlyPolicy>(
  policy: &Policy,
  table: &TableHandleRef,
) -> Result<TreeHeader> {
  policy
    .fetch_slot(HEADER_POINTER, table)?
    .for_read()
    .as_ref()
    .deserialize()
}

pub fn find_record_in_entry<Policy: ReadonlyPolicy>(
  policy: &Policy,
  entry_ptr: Pointer,
  table: &TableHandleRef,
) -> Result<Option<(ReadonlySlot, VersionRecordView)>> {
  let mut next = Some(entry_ptr);
  while let Some(ptr) = next.take() {
    let slot = policy.fetch_slot(ptr, table)?.for_read();
    let entry = slot.as_ref().view::<DataEntryView>()?;

    if let Some(record) =
      entry.find(|record| policy.is_visible(record.owner, record.version))?
    {
      return Ok(Some((slot, record)));
    };
    next = entry.get_next();
  }
  Ok(None)
}

pub enum FindRecordInTree {
  NotFound,
  Found(ReadonlySlot, VersionRecordView),
  Invisible(Option<Pointer>),
}

pub fn find_record_in_tree<Policy: ReadonlyPolicy>(
  policy: &Policy,
  key: StaticKeyRef,
  table: &TableHandleRef,
) -> Result<FindRecordInTree> {
  let mut ptr = read_header(policy, table)?.get_root();
  loop {
    let slot = policy.fetch_slot(ptr, table)?.for_read();
    match slot.as_ref().view::<BTreeNodeView>()? {
      BTreeNodeView::Internal(node) => ptr = node.find(key)?.unwrap_or_else(|i| i),
      BTreeNodeView::Leaf(node) => match node.find(key)? {
        NodeFindResult::NotFound(_) => return Ok(FindRecordInTree::NotFound),
        NodeFindResult::Move(next) => ptr = next,
        NodeFindResult::Found(_, record, entry_ptr) => {
          if !policy.is_visible(record.owner, record.version) {
            return Ok(FindRecordInTree::Invisible(entry_ptr));
          }
          return Ok(FindRecordInTree::Found(slot, record));
        }
      },
    }
  }
}
