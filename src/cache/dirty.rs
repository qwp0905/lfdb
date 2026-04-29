use std::{panic::RefUnwindSafe, sync::Arc};

use crossbeam_skiplist::SkipMap;

use crate::table::{TableHandle, TableId};

pub struct DirtyTables(SkipMap<TableId, Arc<TableHandle>>);
impl DirtyTables {
  pub fn new() -> Self {
    Self(Default::default())
  }

  #[inline]
  pub fn mark(&self, table: &Arc<TableHandle>) {
    self
      .0
      .get_or_insert_with(table.metadata().get_id(), || table.clone());
  }

  #[inline]
  pub fn drain(&self) -> impl Iterator<Item = Arc<TableHandle>> + '_ {
    DirtyTablesIter { inner: self }
  }
}
impl RefUnwindSafe for DirtyTables {}

pub struct DirtyTablesIter<'a> {
  inner: &'a DirtyTables,
}
impl<'a> Iterator for DirtyTablesIter<'a> {
  type Item = Arc<TableHandle>;

  fn next(&mut self) -> Option<Self::Item> {
    self.inner.0.pop_front().map(|v| v.value().clone())
  }
}
