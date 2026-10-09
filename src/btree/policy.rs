use crate::{
  blob::{BlobAppendGuard, BlobId, BlobLen, BlobOffset},
  cache::{CachedSlot, PreparedSlot},
  disk::Pointer,
  objects::Serializable,
  page::AlignedBuf,
  table::TableHandleRef,
  wal::TxId,
  Result,
};

/**
 * Read-only capabilities required by cursor traversal.
 *
 * This keeps B-tree/object logic independent from concrete transaction, cache,
 * and blob-storage implementations. A read policy answers MVCC visibility
 * questions and provides access to cached pages and blob values.
 */
pub trait ReadonlyPolicy {
  fn is_aborted(&self, owner: TxId) -> bool;
  fn is_owned(&self, owner: TxId) -> bool;
  fn is_readable(&self, version: TxId) -> bool;
  fn is_active(&self, owner: TxId) -> bool;
  fn read_blob(
    &self,
    blob_id: BlobId,
    offset: BlobOffset,
    len: BlobLen,
  ) -> Result<AlignedBuf>;

  fn fetch_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<Option<CachedSlot<'_>>>;

  /**
   * Common MVCC visibility rule shared by all read policies. Implementations
   * provide the primitive state checks; the composition is kept here so every
   * cursor read follows the same rule.
   */
  fn is_visible(&self, owner: TxId, version: TxId) -> bool {
    if self.is_owned(owner) {
      return true;
    }
    self.is_readable(version) && !self.is_active(owner) && !self.is_aborted(owner)
  }
}
impl<Policy: ReadonlyPolicy> ReadonlyPolicy for &Policy {
  fn is_aborted(&self, owner: TxId) -> bool {
    (*self).is_aborted(owner)
  }
  fn is_owned(&self, owner: TxId) -> bool {
    (*self).is_owned(owner)
  }
  fn is_readable(&self, version: TxId) -> bool {
    (*self).is_readable(version)
  }
  fn is_active(&self, owner: TxId) -> bool {
    (*self).is_active(owner)
  }
  fn fetch_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<Option<CachedSlot<'_>>> {
    (*self).fetch_slot(pointer, table)
  }
  fn read_blob(
    &self,
    blob_id: BlobId,
    offset: BlobOffset,
    len: BlobLen,
  ) -> Result<AlignedBuf> {
    (*self).read_blob(blob_id, offset, len)
  }
}

/**
 * Write capabilities required by page mutation.
 *
 * Extends read-only traversal with blob writes, page allocation, and the
 * serialize-and-log operation that updates a page while recording the change in
 * WAL.
 */
pub trait WritablePolicy: ReadonlyPolicy {
  fn write_blob(&self, data: Vec<u8>) -> Result<BlobAppendGuard<'_>>;
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut PreparedSlot,
    data: &T,
    table: &TableHandleRef,
  ) -> Result;
  fn alloc_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<Option<CachedSlot<'_>>>;

  /**
   * Reused pointers may still have existing disk contents, so fetch through the
   * normal read path. Newly allocated file-end pointers have no meaningful old
   * contents and can be allocated in cache without a disk read.
   */
  fn alloc_and_log<T: Serializable + Sync>(
    &self,
    data: &T,
    table: &TableHandleRef,
  ) -> Result<Option<Pointer>>
  where
    Self: Sync,
  {
    let Some(slot) = self.try_alloc_slot(table)? else {
      return Ok(None);
    };
    let mut slot = slot.for_write();
    let Some(prepared) = slot.prepare() else {
      table.free().dealloc(slot.get_pointer());
      return Ok(None);
    };
    self.serialize_and_log(prepared, data, table)?;
    Ok(Some(slot.get_pointer()))
  }
  fn try_alloc_slot(&self, table: &TableHandleRef) -> Result<Option<CachedSlot<'_>>>
  where
    Self: Sync,
  {
    let ptr = table.free().alloc();
    let Some(slot) = self.alloc_slot(ptr, table)? else {
      table.free().dealloc(ptr);
      return Ok(None);
    };
    Ok(Some(slot))
  }
}
impl<Policy: WritablePolicy> WritablePolicy for &Policy {
  fn write_blob(&self, data: Vec<u8>) -> Result<BlobAppendGuard<'_>> {
    (*self).write_blob(data)
  }
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut PreparedSlot,
    data: &T,
    table: &TableHandleRef,
  ) -> Result {
    (*self).serialize_and_log(slot, data, table)
  }
  fn alloc_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<Option<CachedSlot<'_>>> {
    (*self).alloc_slot(pointer, table)
  }
}

pub enum ResolvedConflict {
  DeadLock,
  Closed,
}

/**
 * Record-creation capabilities required by insert/update paths.
 *
 * Extends writable access with conflict detection, waiting for conflicting
 * owners, and the current transaction/record identity needed to create new
 * version records.
 */
pub trait CreatablePolicy: WritablePolicy {
  fn current_owner(&self) -> TxId;
  fn current_version(&self) -> TxId;
  fn resolve_conflict(&self, owner: TxId) -> ResolvedConflict;
}
impl<Policy: CreatablePolicy> CreatablePolicy for &Policy {
  fn resolve_conflict(&self, owner: TxId) -> ResolvedConflict {
    (*self).resolve_conflict(owner)
  }
  fn current_owner(&self) -> TxId {
    (*self).current_owner()
  }
  fn current_version(&self) -> TxId {
    (*self).current_version()
  }
}
