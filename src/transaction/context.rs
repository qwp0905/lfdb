use std::sync::{
  atomic::{AtomicBool, Ordering},
  Arc,
};

use crate::{
  cache::WritableSlot,
  cursor::{CreatablePolicy, ReadonlyPolicy, WritablePolicy},
  disk::Pointer,
  serialize::Serializable,
  table::TableHandle,
  wal::TxId,
  Result,
};

use super::{TxOrchestrator, TxSnapshot, TxState};

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
impl<'a> ReadonlyPolicy for &TxContext<'a> {
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
    table: &Arc<TableHandle>,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.orchestrator.fetch(pointer, table.clone())
  }
}
impl<'a> WritablePolicy for &TxContext<'a> {
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut WritableSlot<'_>,
    data: &T,
    table: &Arc<TableHandle>,
  ) -> Result {
    self.orchestrator.serialize_and_log(
      self.state.get_id(),
      table.metadata().get_id(),
      slot,
      data,
    )?;
    self.modified.fetch_or(true, Ordering::Release);
    Ok(())
  }

  fn when_update_entry(&self, pointer: Pointer, table: &Arc<TableHandle>) {
    self.orchestrator.mark_gc(table.clone(), pointer);
  }

  fn alloc_slot(
    &self,
    pointer: Pointer,
    table: &Arc<TableHandle>,
  ) -> Result<crate::cache::CachedSlot<'_>> {
    self.orchestrator.alloc(pointer, table.clone())
  }
}
impl<'a> CreatablePolicy for &TxContext<'a> {
  fn current_owner(&self) -> TxId {
    self.state.get_id()
  }
  fn current_version(&self) -> TxId {
    self.orchestrator.current_version()
  }
  fn wait_close(&self, owner: TxId) {
    self.orchestrator.wait_commit(owner);
  }
}
