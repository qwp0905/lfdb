use std::sync::atomic::{AtomicBool, Ordering};

use crate::{
  cache::RefedSlot,
  cursor::{CreatablePolicy, ReadonlyPolicy, WritablePolicy},
  disk::Pointer,
  objects::Serializable,
  table::TableHandleRef,
  wal::TxId,
  Result,
};

use super::{TxOrchestrator, TxSnapshot, TxState};

/**
 * B-tree policy for a user transaction.
 *
 * `TxContext` carries the transaction state, its visibility snapshot, and the
 * storage hooks needed by cursor/B-tree operations.
 */
pub struct TxContext<'a> {
  orchestrator: &'a TxOrchestrator,
  state: TxState<'a>,
  snapshot: TxSnapshot<'a>,
  modified: AtomicBool,
}
impl<'a> TxContext<'a> {
  #[inline]
  pub const fn new(
    orchestrator: &'a TxOrchestrator,
    state: TxState<'a>,
    snapshot: TxSnapshot<'a>,
  ) -> Self {
    Self {
      orchestrator,
      state,
      snapshot,
      modified: AtomicBool::new(false),
    }
  }

  #[inline]
  pub fn is_available(&self) -> bool {
    self.state.is_available()
  }

  #[inline]
  pub fn is_modified(&self) -> bool {
    self.modified.load(Ordering::Acquire)
  }

  #[inline]
  pub const fn state(&self) -> &'_ TxState<'a> {
    &self.state
  }
}

impl<'a> ReadonlyPolicy for TxContext<'a> {
  fn is_aborted(&self, owner: TxId) -> bool {
    self.snapshot.is_aborted(&owner)
  }
  fn is_owned(&self, owner: TxId) -> bool {
    self.state.get_id() == owner
  }
  fn is_readable(&self, version: TxId) -> bool {
    version <= self.state.get_id()
  }
  fn is_active(&self, owner: TxId) -> bool {
    self.snapshot.is_active(&owner)
  }
  fn fetch_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.orchestrator.fetch(pointer, table)
  }
  fn read_blob(
    &self,
    blob_id: crate::blob::BlobId,
    offset: crate::blob::BlobOffset,
    len: crate::blob::BlobLen,
  ) -> Result<crate::disk::AlignedBuf> {
    let blob = self
      .orchestrator
      .get_blob_handle(blob_id)
      .unwrap_or_else(|| unreachable!("blob id {blob_id} must exists"));
    blob.read_at(offset, len)
  }
}
impl<'a> WritablePolicy for TxContext<'a> {
  /**
   * Mark the transaction modified only after a WAL page record is written.
   *
   * A transaction needs commit/abort visibility handling once it can be observed
   * during WAL replay. Reads and non-WAL work do not make the transaction
   * replay-visible.
   */
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut RefedSlot,
    data: &T,
    table: &TableHandleRef,
  ) -> Result {
    self.orchestrator.serialize_and_log(
      self.state.get_id(),
      table.get_id(),
      self.current_version(),
      slot,
      data,
    )?;
    self.modified.fetch_or(true, Ordering::Release);
    Ok(())
  }

  fn alloc_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.orchestrator.alloc(pointer, table)
  }
  fn write_blob(&self, data: Vec<u8>) -> Result<crate::blob::BlobAppendGuard<'_>> {
    self.orchestrator.write_blob(data)
  }
}
impl<'a> CreatablePolicy for TxContext<'a> {
  fn current_owner(&self) -> TxId {
    self.state.get_id()
  }
  fn current_version(&self) -> TxId {
    self.state.current_version()
  }
  fn resolve_conflict(&self, owner: TxId) -> crate::cursor::ResolvedConflict {
    self
      .orchestrator
      .resolve_conflict(owner, self.state.get_id())
  }
}
