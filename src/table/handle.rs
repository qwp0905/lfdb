use std::{
  mem::{forget, transmute, ManuallyDrop},
  ops::Deref,
  sync::atomic::{AtomicBool, Ordering},
};

use super::{TableId, TableMetadata, TableName};
use crate::{
  disk::{DiskHandle, FreeList, PAGE_SIZE},
  utils::{ExclusivePin, ExclusiveToken, SBox, SharedToken},
  Result,
};

pub type TableHandleRef = SBox<TableHandle>;

pub struct TableHandle {
  id: TableId,
  name: TableName,
  disk: DiskHandle<PAGE_SIZE>,
  free_list: FreeList,
  /**
   * pin for background mutation (eg. compaction / gc)
   */
  pin: ExclusivePin,
  closed: AtomicBool,
}
impl TableHandle {
  pub fn new(metadata: &TableMetadata, disk: DiskHandle<PAGE_SIZE>) -> Self {
    Self {
      id: metadata.get_id(),
      name: metadata.get_name().clone(),
      disk,
      free_list: FreeList::new(),
      pin: ExclusivePin::new(),
      closed: AtomicBool::new(false),
    }
  }

  #[inline]
  pub fn replay(&self) -> Result {
    self.free_list.replay(self.disk.len()?);
    Ok(())
  }

  pub const fn get_name(&self) -> &TableName {
    &self.name
  }
  pub const fn get_id(&self) -> TableId {
    self.id
  }

  #[inline(always)]
  pub const fn disk(&self) -> &DiskHandle<PAGE_SIZE> {
    &self.disk
  }
  #[inline(always)]
  pub const fn free(&self) -> &FreeList {
    &self.free_list
  }

  /**
   * Permanently and exclusively fix the table pin.
   * After calling this method, you cannot pin it forever.
   */
  #[inline]
  pub fn try_close(&self) -> bool {
    if self.pin.try_exclusive().map(forget).is_none() {
      return false;
    }
    self.closed.fetch_or(true, Ordering::Release);
    true
  }

  pub fn is_closed(&self) -> bool {
    self.closed.load(Ordering::Acquire)
  }

  #[inline]
  pub fn truncate(&self) -> Result {
    self.disk.truncate()
  }
}

impl TableHandleRef {
  pub fn try_pin(&self) -> Option<PinnedHandle> {
    let token = self.pin.try_shared()?;
    // transmute allowed since sbox guarantees the lifespan
    Some(PinnedHandle {
      handle: self.clone(),
      token: ManuallyDrop::new(unsafe { transmute(token) }),
    })
  }

  pub fn try_mutation(&self) -> Option<MutationHandle> {
    let token = self.pin.try_exclusive()?;
    // transmute allowed since sbox guarantees the lifespan
    Some(MutationHandle {
      handle: self.clone(),
      token: ManuallyDrop::new(unsafe { transmute(token) }),
    })
  }
}

pub struct PinnedHandle {
  handle: TableHandleRef,
  token: ManuallyDrop<SharedToken<'static>>,
}
impl PinnedHandle {
  #[inline]
  pub fn handle(&self) -> TableHandleRef {
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

pub struct MutationHandle {
  handle: TableHandleRef,
  token: ManuallyDrop<ExclusiveToken<'static>>,
}
impl MutationHandle {
  #[inline]
  pub fn handle(&self) -> &TableHandleRef {
    &self.handle
  }

  pub fn into_inner(self) -> TableHandleRef {
    self.handle.clone()
  }
}

impl Deref for MutationHandle {
  type Target = TableHandle;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.handle
  }
}
impl Drop for MutationHandle {
  fn drop(&mut self) {
    unsafe { ManuallyDrop::drop(&mut self.token) };
  }
}
