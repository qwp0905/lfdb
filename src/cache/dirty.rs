use crossbeam_skiplist::{map::Entry, SkipMap};

use crate::table::{TableHandleRef, TableId};

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

  pub fn get_all(&self) -> impl Iterator<Item = DirtyTableRef<'_>> + '_ {
    self.0.iter().map(DirtyTableRef)
  }
}

pub struct DirtyTableRef<'a>(Entry<'a, TableId, TableHandleRef>);
impl<'a> DirtyTableRef<'a> {
  pub fn as_table(&self) -> &TableHandleRef {
    self.0.value()
  }
  pub fn finish(self) {
    self.0.remove();
  }
}
