use std::{ops::Deref, sync::Arc};

use super::TableMetadata;
use crate::{
  disk::{DiskController, FreeList, PAGE_SIZE},
  utils::{ExclusivePin, ExclusiveToken, SharedToken},
  Error, Result,
};

pub struct TableHandle {
  metadata: TableMetadata,
  disk: DiskController<PAGE_SIZE>,
  free_list: FreeList,
  pin: ExclusivePin,
}
impl TableHandle {
  pub fn new(metadata: TableMetadata, disk: DiskController<PAGE_SIZE>) -> Self {
    Self {
      metadata,
      disk,
      free_list: FreeList::new(),
      pin: ExclusivePin::new(),
    }
  }

  pub fn try_pin<'a>(self: &'a Arc<Self>) -> Option<PinnedHandle<'a>> {
    let token = self.pin.try_shared()?;
    Some(PinnedHandle {
      handle: self,
      _token: token,
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

  #[inline]
  pub fn closed(&self) -> bool {
    self.pin.is_exclusive()
  }

  #[inline]
  pub fn try_close(&self) -> Option<ExclusiveToken<'_>> {
    self.pin.try_exclusive()
  }

  pub fn truncate(&self) -> Result {
    std::fs::remove_file(self.metadata.get_path()).map_err(Error::IO)
  }
}

pub struct PinnedHandle<'a> {
  handle: &'a Arc<TableHandle>,
  _token: SharedToken<'a>,
}
impl<'a> PinnedHandle<'a> {
  #[inline]
  pub fn handle(&self) -> Arc<TableHandle> {
    self.handle.clone()
  }
}

impl<'a> Deref for PinnedHandle<'a> {
  type Target = TableHandle;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.handle
  }
}
