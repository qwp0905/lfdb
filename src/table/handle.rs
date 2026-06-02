use std::{
  mem::{forget, transmute},
  ops::Deref,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
};

use super::TableMetadata;
use crate::{
  disk::{DiskController, FreeList, PAGE_SIZE},
  utils::{ExclusivePin, ExclusiveToken, SharedToken},
  Result,
};

pub type TableHandleRef = Arc<TableHandle>;

pub struct TableHandle {
  metadata: TableMetadata,
  disk: DiskController<PAGE_SIZE>,
  free_list: FreeList,
  /**
   * pin for mutation (eg. compaction / gc)
   */
  pin: ExclusivePin,
  closed: AtomicBool,
}
impl TableHandle {
  pub fn new(metadata: &TableMetadata, disk: DiskController<PAGE_SIZE>) -> Self {
    Self {
      metadata: TableMetadata::new(
        metadata.get_id(),
        metadata.get_name().to_string(),
        metadata.get_path().into(),
      ),
      disk,
      free_list: FreeList::new(),
      pin: ExclusivePin::new(),
      closed: AtomicBool::new(false),
    }
  }

  pub fn try_pin(self: &Arc<Self>) -> Option<PinnedHandle> {
    let token = self.pin.try_shared()?;
    // transmute allowed since arc guarantees the lifespan
    Some(PinnedHandle {
      handle: self.clone(),
      _token: unsafe { transmute(token) },
    })
  }

  pub fn try_mutation(self: &Arc<Self>) -> Option<MutationHandle> {
    let token = self.pin.try_exclusive()?;
    // transmute allowed since arc guarantees the lifespan
    Some(MutationHandle {
      handle: self.clone(),
      _token: unsafe { transmute(token) },
    })
  }

  #[inline]
  pub fn replay(&self) -> Result {
    self.free_list.replay(self.disk.len()?);
    Ok(())
  }

  #[inline(always)]
  pub fn metadata(&self) -> &TableMetadata {
    &self.metadata
  }
  #[inline(always)]
  pub fn disk(&self) -> &DiskController<PAGE_SIZE> {
    &self.disk
  }
  #[inline(always)]
  pub fn free(&self) -> &FreeList {
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

pub struct PinnedHandle {
  handle: TableHandleRef,
  _token: SharedToken<'static>,
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

pub struct MutationHandle {
  handle: TableHandleRef,
  _token: ExclusiveToken<'static>,
}
impl MutationHandle {
  #[inline]
  pub fn handle(&self) -> &TableHandleRef {
    &self.handle
  }

  pub fn into_inner(self) -> TableHandleRef {
    self.handle
  }
}

impl Deref for MutationHandle {
  type Target = TableHandle;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.handle
  }
}
