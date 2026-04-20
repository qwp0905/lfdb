use std::{
  mem::forget,
  ops::Deref,
  sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
  },
};

use super::TableMetadata;
use crate::{
  disk::{DiskController, FreeList, PAGE_SIZE},
  utils::{ExclusivePin, SharedToken},
  Error, Result,
};

pub struct TableHandle {
  metadata: TableMetadata,
  disk: DiskController<PAGE_SIZE>,
  free_list: FreeList,
  pin: ExclusivePin,
  redirection_count: AtomicU32,
}
impl TableHandle {
  pub fn new(metadata: TableMetadata, disk: DiskController<PAGE_SIZE>) -> Self {
    Self {
      metadata,
      disk,
      free_list: FreeList::new(),
      pin: ExclusivePin::new(),
      redirection_count: AtomicU32::new(0),
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

  /**
   * Permanently and exclusively fix the table pin.
   * After calling this method, you cannot pin it forever.
   */
  #[inline]
  pub fn try_close(&self) -> bool {
    self.pin.try_exclusive().map(forget).is_some()
  }

  pub fn truncate(&self) -> Result {
    std::fs::remove_file(self.metadata.get_path()).map_err(Error::IO)
  }

  pub fn mark_redirection(&self) {
    self.redirection_count.fetch_add(1, Ordering::Relaxed);
  }

  pub fn dead_ratio(&self) -> f64 {
    (self.free_list.dead_count()
      + self.redirection_count.load(Ordering::Relaxed) as usize) as f64
      / self.free_list.file_len() as f64
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
