use std::mem::replace;

use crate::disk::{PageRef, Pointer, PAGE_SIZE};

pub struct Frame {
  page: PageRef<PAGE_SIZE>,
  /**
   * pointer can be wrong if nothing allocated.
   * only lru table is the single truth source.
   */
  pointer: Pointer,
}
impl Frame {
  #[inline]
  pub fn new(pointer: Pointer, page: PageRef<PAGE_SIZE>) -> Self {
    Self { page, pointer }
  }
  #[inline]
  pub fn empty(page: PageRef<PAGE_SIZE>) -> Self {
    Self::new(0, page)
  }
  #[inline]
  pub fn replace(
    &mut self,
    pointer: Pointer,
    page: PageRef<PAGE_SIZE>,
  ) -> PageRef<PAGE_SIZE> {
    self.pointer = pointer;
    replace(&mut self.page, page)
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
}
