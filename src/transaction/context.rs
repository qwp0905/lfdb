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
  fn is_visible(&self, owner: crate::wal::TxId, version: crate::wal::TxId) -> bool {
    let current = self.state.get_id();
    if owner == current {
      return true;
    }
    version <= current && self.snapshot.is_visible(&owner)
  }

  fn fetch_slot(
    &self,
    pointer: Pointer,
    table: &Arc<TableHandle>,
  ) -> Result<crate::cache::CacheSlot<'_>> {
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
}
impl<'a> CreatablePolicy for &TxContext<'a> {
  fn is_conflict(&self, owner: crate::wal::TxId) -> bool {
    owner != self.state.get_id() && self.snapshot.is_active(&owner)
  }
  fn current_owner(&self) -> TxId {
    self.state.get_id()
  }
  fn current_version(&self) -> TxId {
    self.orchestrator.current_version()
  }
}
