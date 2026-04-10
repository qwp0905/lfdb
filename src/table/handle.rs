use std::{
  ops::Deref,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
};

use crossbeam::utils::Backoff;

use super::TableMetadata;
use crate::{
  disk::{DiskController, FreeList, PagePool, PAGE_SIZE},
  metrics::MetricsRegistry,
  Error, Result,
};

const CLOSED: usize = 1 << (usize::BITS - 1);

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

  fn pin(&self) {
    self.pin.fetch_add(1, Ordering::Release);
  }
  fn unpin(&self) {
    self.pin.fetch_sub(1, Ordering::Release);
  }

  pub fn try_pin(self: Arc<Self>) -> Option<PinnedHandle> {
    let backoff = Backoff::new();
    loop {
      let c = self.pin.load(Ordering::Acquire);
      if c & CLOSED != 0 {
        return None;
      }

      if self
        .pin
        .compare_exchange(c, c + 1, Ordering::Release, Ordering::Acquire)
        .is_ok()
      {
        return Some(PinnedHandle(Arc::clone(&self)));
      }

      backoff.spin();
    }
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
    self.pin.load(Ordering::Acquire) & CLOSED != 0
  }

  pub fn try_close(&self) -> bool {
    self
      .pin
      .compare_exchange(0, CLOSED, Ordering::Release, Ordering::Acquire)
      .is_ok()
  }

  pub fn truncate(&self) -> Result {
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
