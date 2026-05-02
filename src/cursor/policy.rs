use std::sync::Arc;

use crate::{
  cache::{CacheSlot, WritableSlot},
  disk::Pointer,
  serialize::Serializable,
  table::TableHandle,
  wal::TxId,
  Result,
};

pub trait ReadonlyPolicy {
  fn is_visible(&self, owner: TxId, version: TxId) -> bool;
  fn fetch_slot(
    &self,
    pointer: Pointer,
    table: &Arc<TableHandle>,
  ) -> Result<CacheSlot<'_>>;
}

pub trait WritablePolicy: ReadonlyPolicy {
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut WritableSlot<'_>,
    data: &T,
    table: &Arc<TableHandle>,
  ) -> Result;

  fn alloc_and_log<T: Serializable>(
    &self,
    data: &T,
    table: &Arc<TableHandle>,
  ) -> Result<Pointer> {
    let ptr = table.free().alloc();
    let mut slot = self.fetch_slot(ptr, table)?.for_write();
    self.serialize_and_log(&mut slot, data, table)?;
    Ok(ptr)
  }
  fn when_update_entry(&self, entry_pointer: Pointer, table: &Arc<TableHandle>);
}

pub trait CreatablePolicy: WritablePolicy {
  fn is_conflict(&self, owner: TxId) -> bool;
  fn current_owner(&self) -> TxId;
  fn current_version(&self) -> TxId;
}
