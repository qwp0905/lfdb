use std::{
  mem::{forget, transmute, ManuallyDrop},
  ops::Deref,
};

use super::{TableId, TableMetadata, TableName};
use crate::{
  disk::{BlockIOHandle, FreeList, PAGE_SIZE},
  utils::{ExclusivePin, SBox, SharedToken},
  Result,
};

pub type TableHandleRef = SBox<TableHandle>;

/**
 * Runtime accessor for one opened table segment.
 *
 * `TableMetadata` is the durable descriptor. `TableHandle` is the live object
 * built from it: it owns the block IO handle, the in-memory free list, and the
 * pin used to keep background table operations from racing with final close.
 */
pub struct TableHandle {
  id: TableId,
  name: TableName,
  disk: BlockIOHandle<PAGE_SIZE>,
  free_list: FreeList,
  /**
   * pin to protect background mutation (eg. compaction / gc) from drop
   */
  pin: ExclusivePin,
}
impl TableHandle {
  pub fn new(metadata: &TableMetadata, disk: BlockIOHandle<PAGE_SIZE>) -> Self {
    Self {
      id: metadata.get_id(),
      name: metadata.get_name().clone(),
      disk,
      free_list: FreeList::new(),
      pin: ExclusivePin::new(),
    }
  }

  pub const fn get_name(&self) -> &TableName {
    &self.name
  }
  pub const fn get_id(&self) -> TableId {
    self.id
  }

  #[inline(always)]
  pub const fn disk(&self) -> &BlockIOHandle<PAGE_SIZE> {
    &self.disk
  }
  #[inline(always)]
  pub const fn free(&self) -> &FreeList {
    &self.free_list
  }

  /**
   * Try to logically close this table segment.
   *
   * This does not close the underlying file handle. It permanently takes the
   * exclusive table pin so no new `PinnedHandle` can be created. Drop-table and
   * other lifecycle code use this as the announcement that runtime-independent
   * background workers should stop touching this table.
   */
  #[inline]
  pub fn try_close(&self) -> bool {
    if self.pin.try_exclusive().map(forget).is_none() {
      return false;
    }
    true
  }

  #[inline]
  pub fn truncate(&self) -> Result {
    self.disk.truncate()
  }
}

impl TableHandleRef {
  pub fn try_pin(&self) -> Option<PinnedHandle> {
    let token = self.pin.try_shared()?;
    let static_token =
      unsafe { transmute::<SharedToken<'_>, SharedToken<'static>>(token) };
    // SAFETY: `PinnedHandle` stores a clone of this `SBox<TableHandle>` together
    // with the shared token. The clone keeps the allocation alive until the token
    // is dropped in `PinnedHandle::drop`, so extending the token lifetime is valid
    // for the guard's lifetime.
    Some(PinnedHandle {
      handle: self.clone(),
      token: ManuallyDrop::new(static_token),
    })
  }
}

/**
 * Guarded table handle.
 *
 * Holds a shared table pin together with a cloned table reference, preventing
 * `try_close` from taking the exclusive pin while the guard is alive.
 *
 * TODO: This can keep a table pinned longer than necessary for long background
 * work. Prefer short-lived pins at the actual access points instead of extending
 * the pin lifetime across waits.
 */
pub struct PinnedHandle {
  handle: TableHandleRef,
  token: ManuallyDrop<SharedToken<'static>>,
}
impl PinnedHandle {
  #[inline]
  pub const fn handle(&self) -> &TableHandleRef {
    &self.handle
  }

  pub fn into_inner(self) -> TableHandleRef {
    self.handle.clone()
  }
}

impl Deref for PinnedHandle {
  type Target = TableHandle;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.handle
  }
}
impl Drop for PinnedHandle {
  fn drop(&mut self) {
    unsafe { ManuallyDrop::drop(&mut self.token) };
  }
}
