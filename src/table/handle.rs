use std::{mem::forget, ops::Deref};

use crossbeam_skiplist::{map::Entry, SkipMap};

use super::{TableId, TableMetadata, TableName};
use crate::{
  disk::{BlockIOHandle, FreeList, PAGE_SIZE},
  utils::{ExclusivePin, SBox, SharedToken},
  wal::TxId,
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

  /**
   * local guard for detect write conflict per each key.
   */
  conflict_set: SkipMap<Vec<u8>, TxId>,
}
impl TableHandle {
  pub fn new(metadata: &TableMetadata, disk: BlockIOHandle<PAGE_SIZE>) -> Self {
    Self {
      id: metadata.get_id(),
      name: metadata.get_name().clone(),
      disk,
      free_list: FreeList::new(),
      pin: ExclusivePin::new(),
      conflict_set: SkipMap::new(),
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

  /**
   * reserve for detect write conflict
   */
  pub fn reserve(
    &self,
    key: Vec<u8>,
    owner: TxId,
  ) -> std::result::Result<ReserveGuard<'_>, TxId> {
    let entry = self.conflict_set.get_or_insert(key, owner);
    if *entry.value() != owner {
      return Err(*entry.value());
    }
    Ok(ReserveGuard(entry))
  }
  pub fn is_reserved(&self, key: &[u8]) -> bool {
    self.conflict_set.contains_key(key)
  }
}

pub struct ReserveGuard<'a>(Entry<'a, Vec<u8>, TxId>);
impl<'a> Drop for ReserveGuard<'a> {
  fn drop(&mut self) {
    self.0.remove();
  }
}

impl TableHandleRef {
  pub fn try_pin(&self) -> Option<PinnedHandle<'_>> {
    let token = self.pin.try_shared()?;
    Some(PinnedHandle {
      handle: self,
      _token: token,
    })
  }
}

/**
 * Guarded table handle.
 */
pub struct PinnedHandle<'a> {
  handle: &'a TableHandleRef,
  _token: SharedToken<'a>,
}
impl<'a> PinnedHandle<'a> {
  #[inline]
  pub const fn handle(&self) -> &TableHandleRef {
    self.handle
  }

  pub fn into_inner(self) -> TableHandleRef {
    self.handle.clone()
  }
}

impl<'a> Deref for PinnedHandle<'a> {
  type Target = TableHandle;

  #[inline]
  fn deref(&self) -> &Self::Target {
    self.handle
  }
}
