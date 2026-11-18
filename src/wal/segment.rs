use std::{io, path::PathBuf};

use super::WAL_BLOCK_SIZE;
use crate::{
  disk::{IOHandle, IOPool, PendingIO, Pointer},
  page::PageRef,
  utils::{create_static_ref, uuid_simple},
  Error, Result,
};

pub const FILE_EXT: &str = "log";

pub type FsyncResult = PendingIO;

/**
 * Fixed-size WAL segment made of WAL blocks.
 *
 * `max_len` is the number of WAL blocks in the segment. Each write addresses a
 * block index inside the segment and is translated to a byte offset by
 * multiplying by `WAL_BLOCK_SIZE`.
 */
pub struct WALSegment {
  handle: IOHandle,
}
impl WALSegment {
  const SHIFT: u32 = WAL_BLOCK_SIZE.ilog2();
  pub fn open(max_len: Pointer, pool: &IOPool) -> Result<Self> {
    let filename = PathBuf::from(uuid_simple()).with_extension(FILE_EXT);

    // Pre-allocate the full file space upfront. Segments are rarely created fresh —
    // they are almost always reused via rename(). Paying the allocation cost once
    // at creation avoids metadata updates on every subsequent write.
    let file_len = max_len << Self::SHIFT;
    let handle = pool.open_static_sized(filename, file_len)?;
    handle.fsync_async().wait_flatten()?;

    Ok(Self { handle })
  }
  pub fn submit_write<R, F>(
    &self,
    pointer: Pointer,
    page: PageRef<WAL_BLOCK_SIZE>,
    callback: F,
  ) -> PendingIO<R>
  where
    R: Send + 'static,
    F: FnOnce(io::Result<()>) -> R + Send + 'static,
  {
    // SAFETY: the slice points into the heap buffer of `page`, which the completion
    // closure owns until the write finishes.
    let static_ref = unsafe { create_static_ref::<[u8]>(page.as_slice()) };
    self
      .handle
      .write_async_with(static_ref, pointer << Self::SHIFT, move |result| {
        drop(page);
        callback(result)
      })
  }

  /**
   * Repurposes this segment for a new generation by renaming it in place.
   * Much faster than creating a new file — avoids the fallocate + metadata sync cost.
   */
  pub fn reuse(&self) -> Result {
    let new_path = PathBuf::from(uuid_simple()).with_extension(FILE_EXT);
    self.handle.rename(new_path).map_err(Error::IO)
  }

  #[inline]
  pub fn fsync(&self) -> FsyncResult {
    self.handle.fdatasync_async()
  }

  #[inline]
  pub fn truncate(self) -> Result {
    self.handle.truncate().map_err(Error::IO)?;
    Ok(())
  }
}
