use std::{io::Result as IOResult, path::PathBuf};

use super::WAL_BLOCK_SIZE;
use crate::{
  background::Oneshot,
  disk::{IOHandle, IOPool, Page, Pointer},
  utils::uuid_simple,
  Error, Result,
};

pub const FILE_EXT: &str = "log";

pub type FsyncResult = Oneshot<IOResult<()>>;

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
    let handle = pool.open_direct_io(filename)?;

    // Pre-allocate the full file space upfront. Segments are rarely created fresh —
    // they are almost always reused via rename(). Paying the allocation cost once
    // at creation avoids metadata updates on every subsequent write.
    let file_len = max_len << Self::SHIFT;
    handle.fallocate(0, file_len).map_err(Error::IO)?;
    handle.fsync().map_err(Error::IO)?;

    Ok(Self { handle })
  }
  pub fn write_async(
    &self,
    pointer: Pointer,
    page: &'static Page<WAL_BLOCK_SIZE>,
  ) -> Oneshot<IOResult<()>> {
    // segment must call write only rather than alloc_and_write since it calls fallocate in constructor.
    self
      .handle
      .write_only(page.as_slice(), pointer << Self::SHIFT)
  }

  /**
   * Repurposes this segment for a new generation by renaming it in place.
   * Much faster than creating a new file — avoids the fallocate + metadata sync cost.
   */
  pub fn reuse(&self) -> IOResult<()> {
    let new_path = PathBuf::from(uuid_simple()).with_extension(FILE_EXT);
    self.handle.rename(new_path)
  }

  #[inline]
  pub fn fsync(&self) -> FsyncResult {
    self.handle.fdatasync()
  }

  #[inline]
  pub fn truncate(self) -> Result {
    self.handle.truncate().map_err(Error::IO)?;
    Ok(())
  }
}
