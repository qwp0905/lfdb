use std::iter::repeat;

use crossbeam_skiplist::SkipMap;

use super::BlockId;
use crate::{
  table::{TableHandleRef, TableId},
  utils::AtomicBitmap,
};

/**
 * Table-level dirty marker set.
 *
 * This does not track individual dirty blocks. It only records which table
 * handles currently have work that may need flushing/syncing. Repeated marks for
 * the same table collapse into one entry, and `drain` hands the current marked
 * set to the checkpoint/writeback path.
 */
pub struct DirtyTables(SkipMap<TableId, TableHandleRef>);
impl DirtyTables {
  pub fn new() -> Self {
    Self(Default::default())
  }

  #[inline]
  pub fn mark(&self, table: &TableHandleRef) {
    self.0.get_or_insert_with(table.get_id(), || table.clone());
  }

  #[inline]
  pub fn drain(&self) -> impl Iterator<Item = TableHandleRef> + '_ {
    repeat(()).map_while(|_| self.0.pop_front().map(|v| v.value().clone()))
  }
}

pub struct DirtyBlocks(AtomicBitmap);
impl DirtyBlocks {
  pub fn new(capacity: usize) -> Self {
    Self(AtomicBitmap::new(capacity))
  }
  pub fn iter(&self) -> impl Iterator<Item = BlockId> + '_ {
    self.0.iter().map(|i| i as BlockId)
  }
  pub fn insert(&self, id: BlockId) -> bool {
    self.0.insert(id as u64)
  }
  pub fn remove(&self, id: BlockId) -> bool {
    self.0.remove(id as u64)
  }
  pub fn contains(&self, id: BlockId) -> bool {
    self.0.contains(id as u64)
  }
}
