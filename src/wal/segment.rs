use std::{mem::transmute, path::Path, sync::Arc};

use super::{SegmentGeneration, WAL_BLOCK_SIZE};
use crate::{
  disk::{IOHandle, IOPool, Page, Pointer},
  error::Result,
  thread::{BackgroundThread, TaskHandle, WorkBuilder},
  utils::{ToArc, ToBox},
  Error,
};

pub const FILE_EXT: &str = "log";

pub type FsyncResult = TaskHandle<Result>;

const SIZE: Pointer = WAL_BLOCK_SIZE as Pointer;

pub struct WALSegment {
  handle: Arc<IOHandle>,
  flush: Box<dyn BackgroundThread<(), Result>>,
}
impl WALSegment {
  pub fn parse_generation(path: &Path) -> Result<SegmentGeneration> {
    let generation =
      unsafe { str::from_utf8_unchecked(path.file_stem().unwrap().as_encoded_bytes()) }
        .parse()
        .map_err(Error::unknown)?;
    Ok(generation)
  }

  pub fn open(
    prefix: &Path,
    generation: SegmentGeneration,
    flush_count: usize,
    max_len: Pointer,
    pool: &IOPool,
  ) -> Result<Self> {
    let path = prefix.join(pad_start(generation)).with_extension(FILE_EXT);
    let handle = pool.create_handle(&path)?.to_arc();

    // Pre-allocate the full file space upfront. Segments are rarely created fresh —
    // they are almost always reused via rename(). Paying the allocation cost once
    // at creation avoids metadata updates on every subsequent write.
    let file_len = max_len * SIZE;
    handle.fallocate(0, file_len)?;
    handle.fsync()?;

    let flush = WorkBuilder::new()
      .name("wal flush")
      .single()
      .eager_buffering(flush_count, handle_flush(handle.clone()))
      .to_box();

    Ok(Self { handle, flush })
  }
  pub fn write(&self, pointer: Pointer, page: &Page<WAL_BLOCK_SIZE>) -> Result {
    // transmute extends the slice lifetime to 'static to satisfy the background thread's
    // type bound. Safe because wait and flatten blocks until the write completes, ensuring
    // the page buffer outlives the background thread's use of the pointer.
    self
      .handle
      .write_async(pointer * SIZE, unsafe { transmute(page.as_ref()) })
      .wait()
  }

  /**
   * Repurposes this segment for a new generation by renaming it in place.
   * Much faster than creating a new file — avoids the fallocate + metadata sync cost.
   */
  pub fn reuse(&self, prefix: &Path, generation: SegmentGeneration) -> Result {
    let new_path = prefix.join(pad_start(generation)).with_extension(FILE_EXT);
    self.handle.rename(&new_path)
  }

  #[inline]
  pub fn fsync(&self) -> FsyncResult {
    self.flush.execute(())
  }

  #[inline]
  pub fn truncate(self) -> Result {
    self.close();
    self.handle.truncate()?;
    Ok(())
  }

  #[inline]
  pub fn close(&self) {
    self.flush.close();
  }
}

#[inline]
const fn handle_flush(file: Arc<IOHandle>) -> impl Fn(Vec<()>) -> Result {
  move |_| file.fdatasync()
}

/**
 * Zero-pad to 20 digits: ensures lexicographic file ordering matches numeric order,
 * and accommodates the full u64 range (max 20 digits).
 */
fn pad_start(n: SegmentGeneration) -> String {
  format!("{:0>20}", n)
}
