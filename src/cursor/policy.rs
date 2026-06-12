use crate::{
  cache::{CachedSlot, RefedSlot},
  disk::{FreePointer, Pointer},
  serialize::Serializable,
  table::TableHandleRef,
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
    table: &TableHandleRef,
  ) -> Result<CachedSlot<'_>>;

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
  ) -> Result<CachedSlot<'_>> {
    (*self).fetch_slot(pointer, table)
  }
}

pub trait WritablePolicy: ReadonlyPolicy {
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut RefedSlot,
    data: &T,
    table: &TableHandleRef,
  ) -> Result;
  fn alloc_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<CachedSlot<'_>>;

  fn alloc_and_log<T: Serializable>(
    &self,
    data: &T,
    table: &TableHandleRef,
  ) -> Result<Pointer> {
    let mut slot = match table.free().alloc() {
      FreePointer::Reuse(ptr) => self.fetch_slot(ptr, table),
      FreePointer::Alloc(ptr) => self.alloc_slot(ptr, table),
    }?
    .for_write();
    self.serialize_and_log(&mut slot, data, table)?;
    Ok(slot.get_pointer())
  }
}
impl<Policy: WritablePolicy> WritablePolicy for &Policy {
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut RefedSlot,
    data: &T,
    table: &TableHandleRef,
  ) -> Result {
    (*self).serialize_and_log(slot, data, table)
  }
  fn alloc_slot(
    &self,
    pointer: Pointer,
    table: &TableHandleRef,
  ) -> Result<CachedSlot<'_>> {
    (*self).alloc_slot(pointer, table)
  }
}

pub trait CreatablePolicy: WritablePolicy {
  fn is_conflict(&self, owner: TxId, version: TxId) -> bool {
    !self.is_owned(owner) && (!self.is_readable(version) || self.is_active(owner))
  }
  fn wait_close(&self, owner: TxId);
  fn current_owner(&self) -> TxId;
  fn current_version(&self) -> TxId;
}
impl<Policy: CreatablePolicy> CreatablePolicy for &Policy {
  fn wait_close(&self, owner: TxId) {
    (*self).wait_close(owner)
  }
  fn current_owner(&self) -> TxId {
    (*self).current_owner()
  }
  fn current_version(&self) -> TxId {
    (*self).current_version()
  }
}
