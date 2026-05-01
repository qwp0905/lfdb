use std::{
  mem::{forget, transmute},
  ops::Deref,
  sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
  },
};

use super::TableMetadata;
use crate::{
  disk::{DiskController, FreeList, PAGE_SIZE},
  utils::{ExclusivePin, ExclusiveToken, SharedToken},
  Result,
};

pub struct TableHandle {
  metadata: TableMetadata,
  disk: DiskController<PAGE_SIZE>,
  free_list: FreeList,
  /**
   * pin for mutation (eg. compaction / merge nodes / gc)
   */
  pin: ExclusivePin,
  closed: AtomicBool,
  redirection_count: AtomicU32,
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
      redirection_count: AtomicU32::new(0),
      closed: AtomicBool::new(false),
    }
  }

  pub fn try_pin<'a>(self: &'a Arc<Self>) -> Option<PinnedHandle<'a>> {
    let token = self.pin.try_shared()?;
    Some(PinnedHandle {
      handle: self,
      _token: token,
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
    self.disk.truncate(self.metadata.get_path())
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

pub struct MutationHandle {
  handle: Arc<TableHandle>,
  _token: ExclusiveToken<'static>,
}
impl MutationHandle {
  #[inline]
  pub fn handle(&self) -> &Arc<TableHandle> {
    &self.handle
  }

  pub fn into_inner(self) -> Arc<TableHandle> {
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
