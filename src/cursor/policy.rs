use std::sync::Arc;

use crate::{
  cache::{CachedSlot, RefedSlot},
  disk::Pointer,
  serialize::Serializable,
  table::TableHandle,
  wal::TxId,
  Result,
};

pub trait ReadonlyPolicy {
  fn is_aborted(&self, owner: TxId) -> bool;
  fn is_owned(&self, owner: TxId) -> bool;
  fn is_readable(&self, version: TxId) -> bool;
  fn is_active(&self, owner: TxId) -> bool;

  fn fetch_slot(
    &self,
    pointer: Pointer,
    table: &Arc<TableHandle>,
  ) -> Result<CachedSlot<'_>>;

  fn is_visible(&self, owner: TxId, version: TxId) -> bool {
    if self.is_owned(owner) {
      return true;
    }
    self.is_readable(version) && !self.is_active(owner) && !self.is_aborted(owner)
  }
}

pub trait WritablePolicy: ReadonlyPolicy {
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut RefedSlot,
    data: &T,
    table: &Arc<TableHandle>,
  ) -> Result;
  fn alloc_slot(
    &self,
    pointer: Pointer,
    table: &Arc<TableHandle>,
  ) -> Result<CachedSlot<'_>>;

  fn alloc_and_log<T: Serializable>(
    &self,
    data: &T,
    table: &Arc<TableHandle>,
  ) -> Result<Pointer> {
    let ptr = table.free().alloc();
    let mut slot = self.alloc_slot(ptr, table)?.for_write();
    self.serialize_and_log(&mut slot, data, table)?;
    Ok(ptr)
  }

  fn when_update_entry(&self, entry_pointer: Pointer, table: &Arc<TableHandle>);
}

pub trait CreatablePolicy: WritablePolicy {
  fn is_conflict(&self, owner: TxId, version: TxId) -> bool {
    !self.is_owned(owner) && (!self.is_readable(version) || self.is_active(owner))
  }
  fn wait_close(&self, owner: TxId);
  fn current_owner(&self) -> TxId;
  fn current_version(&self) -> TxId;
}
