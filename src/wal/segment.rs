use std::{io, path::PathBuf};

use super::WAL_BLOCK_SIZE;
use crate::{
  background::AsyncTask,
  disk::{IOHandle, IOPool, PendingIO, Pointer},
  page::PageRef,
  utils::{create_static_ref, uuid_simple},
  Error, Result,
};

pub const FILE_EXT: &str = "log";

pub type FsyncResult = PendingIO;

pub struct PendingSegment(AsyncTask<io::Result<PendingOperations>>);
impl PendingSegment {
  pub fn resolve(
    self,
  ) -> std::result::Result<WALSegment, (io::Error, Option<WALSegment>)> {
    let joined = match self.0.wait().unwrap() {
      Ok(joined) => joined,
      Err(err) => return Err((err, None)),
    };
    let (pending, segment) = match joined.fallocate.wait() {
      Ok((pending, handle)) => (pending, WALSegment { handle }),
      Err((err, handle)) => return Err((err, Some(WALSegment { handle }))),
    };
    if let Err(err) = pending.wait() {
      return Err((err, Some(segment)));
    }
    if let Err(err) = joined.sync_dir.wait() {
      return Err((err, Some(segment)));
    }
    Ok(segment)
  }

  pub fn into_raw(this: Self) -> *mut () {
    AsyncTask::into_raw(this.0)
  }
  pub const unsafe fn from_raw(raw: *mut ()) -> Self {
    Self(unsafe { AsyncTask::from_raw(raw) })
  }
}

struct PendingOperations {
  fallocate: PendingIO<std::result::Result<(PendingIO, IOHandle), (io::Error, IOHandle)>>,
  sync_dir: PendingIO,
}

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
  pub fn open(max_len: Pointer, pool: &IOPool) -> PendingSegment {
    let filename = PathBuf::from(uuid_simple()).with_extension(FILE_EXT);

    // Pre-allocate the full file space upfront. Segments are rarely created fresh —
    // they are almost always reused via rename(). Paying the allocation cost once
    // at creation avoids metadata updates on every subsequent write.
    let file_len = max_len << Self::SHIFT;
    let task = pool.open_static_sized_with(&filename, move |result| {
      let handle = result?;
      let sync_dir = handle.sync_dir_async();
      let fallocate =
        handle.fallocate_then(0, file_len, move |result, handle| match result {
          Ok(_) => Ok((handle.fsync_async(), handle)),
          Err(err) => Err((err, handle)),
        });
      Ok(PendingOperations {
        fallocate,
        sync_dir,
      })
    });

    PendingSegment(task)
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
