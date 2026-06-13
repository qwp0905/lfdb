use std::iter::repeat;

use crossbeam_skiplist::SkipMap;

use crate::table::{TableHandleRef, TableId};

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
