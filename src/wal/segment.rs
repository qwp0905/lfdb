use std::{mem::transmute, path::PathBuf};

use super::WAL_BLOCK_SIZE;
use crate::{
  disk::{IOHandle, IOPool, Page, Pointer},
  error::Result,
  thread::TaskHandle,
  utils::uuid_simple,
};

pub const FILE_EXT: &str = "log";

pub type FsyncResult = TaskHandle<()>;

const SIZE: Pointer = WAL_BLOCK_SIZE as Pointer;

pub struct WALSegment {
  handle: IOHandle,
}
impl WALSegment {
  pub fn open(max_len: Pointer, pool: &IOPool) -> Result<Self> {
    let filename = PathBuf::from(uuid_simple()).with_extension(FILE_EXT);
    let handle = pool.open_direct_io(filename)?;

    // Pre-allocate the full file space upfront. Segments are rarely created fresh —
    // they are almost always reused via rename(). Paying the allocation cost once
    // at creation avoids metadata updates on every subsequent write.
    let file_len = max_len * SIZE;
    handle.fallocate(0, file_len)?;
    handle.fsync()?;

    Ok(Self { handle })
  }
  pub fn write(&self, pointer: Pointer, page: &Page<WAL_BLOCK_SIZE>) -> Result {
    // transmute extends the slice lifetime to 'static to satisfy the background thread's
    // type bound. Safe because wait and flatten blocks until the write completes, ensuring
    // the page buffer outlives the background thread's use of the pointer.
    self
      .handle
      .write_async(unsafe { transmute(page.as_ref()) }, pointer * SIZE)
      .wait()
  }

  /**
   * Repurposes this segment for a new generation by renaming it in place.
   * Much faster than creating a new file — avoids the fallocate + metadata sync cost.
   */
  pub fn reuse(&self) -> Result {
    let new_path = PathBuf::from(uuid_simple()).with_extension(FILE_EXT);
    self.handle.rename(new_path)
  }

  #[inline]
  pub fn fsync(&self) -> FsyncResult {
    self.handle.fdatasync()
  }

  #[inline]
  pub fn truncate(self) -> Result {
    self.handle.truncate()?;
    Ok(())
  }
}
