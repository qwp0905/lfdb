use std::{mem::replace, sync::Arc};

use crate::{
  disk::{PageRef, Pointer, WriteAsync, PAGE_SIZE},
  table::TableHandle,
};

pub struct Frame {
  page: PageRef<PAGE_SIZE>,
  /**
   * pointer can be wrong if nothing allocated.
   * only lru table is the single truth source.
   */
  pointer: Pointer,
  handle: Arc<TableHandle>,
}
impl Frame {
  #[inline]
  pub fn new(
    pointer: Pointer,
    page: PageRef<PAGE_SIZE>,
    handle: Arc<TableHandle>,
  ) -> Self {
    Self {
      page,
      pointer,
      handle,
    }
  }

  #[inline]
  pub fn replace(
    &mut self,
    pointer: Pointer,
    page: PageRef<PAGE_SIZE>,
    handle: Arc<TableHandle>,
  ) -> (PageRef<PAGE_SIZE>, Arc<TableHandle>) {
    self.pointer = pointer;
    let old_page = replace(&mut self.page, page);
    let old_handle = replace(&mut self.handle, handle);
    (old_page, old_handle)
  }
  #[inline]
  pub fn get_pointer(&self) -> Pointer {
    self.pointer
  }
  #[inline]
  pub fn page_ref(&self) -> &PageRef<PAGE_SIZE> {
    &self.page
  }
  #[inline]
  pub fn page_ref_mut(&mut self) -> &mut PageRef<PAGE_SIZE> {
    &mut self.page
  }

  pub fn flush_async(&self) -> WriteAsync<PAGE_SIZE> {
    self.handle.disk().write_async(self.pointer, &self.page)
  }

  pub fn handle(&self) -> Arc<TableHandle> {
    self.handle.clone()
  }
}
