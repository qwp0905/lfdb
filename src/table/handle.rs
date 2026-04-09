use std::{
  ops::Deref,
  sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
  },
};

use super::TableMetadata;
use crate::{
  disk::{DiskController, FreeList, PagePool, PAGE_SIZE},
  metrics::MetricsRegistry,
  Error, Result,
};

pub struct TableHandle {
  metadata: TableMetadata,
  disk: DiskController<PAGE_SIZE>,
  free_list: FreeList,
  pin: AtomicUsize,
  closed: AtomicBool,
}
impl TableHandle {
  pub fn open(
    metadata: TableMetadata,
    page_pool: Arc<PagePool<PAGE_SIZE>>,
    metrics: Arc<MetricsRegistry>,
  ) -> Result<Self> {
    let disk = DiskController::open(metadata.get_path(), page_pool, metrics)?;
    Ok(Self {
      metadata,
      disk,
      free_list: FreeList::new(),
      pin: AtomicUsize::new(0),
      closed: AtomicBool::new(false),
    })
  }

  fn pin(&self) {
    self.pin.fetch_add(1, Ordering::Release);
  }
  fn unpin(&self) {
    self.pin.fetch_sub(1, Ordering::Release);
  }

  pub fn is_pinned(&self) -> bool {
    self.pin.load(Ordering::Acquire) > 0
  }

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

  pub fn closed(&self) -> bool {
    self.closed.load(Ordering::Acquire)
  }

  pub fn truncate(&self) -> Result {
    if self.closed.fetch_or(true, Ordering::Release) {
      return Ok(());
    }
    self.disk.close();
    std::fs::remove_file(self.metadata.get_path()).map_err(Error::IO)
  }
}

pub struct PinnedHandle(Arc<TableHandle>);
impl PinnedHandle {
  #[inline]
  pub fn new(handle: Arc<TableHandle>) -> Self {
    handle.pin();
    Self(handle)
  }

  #[inline]
  pub fn handle(&self) -> Arc<TableHandle> {
    self.0.clone()
  }
}
impl Drop for PinnedHandle {
  #[inline]
  fn drop(&mut self) {
    self.0.unpin();
  }
}

impl Deref for PinnedHandle {
  type Target = TableHandle;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.0
  }
}
