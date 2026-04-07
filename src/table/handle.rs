use std::sync::{
  atomic::{AtomicUsize, Ordering},
  Arc,
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
    })
  }

  pub fn pin(&self) {
    self.pin.fetch_add(1, Ordering::Release);
  }
  pub fn unpin(&self) {
    self.pin.fetch_sub(1, Ordering::Release);
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

  pub fn truncate(&self) -> Result {
    self.disk.close();
    std::fs::remove_file(self.metadata.get_path()).map_err(Error::IO)
  }
}
