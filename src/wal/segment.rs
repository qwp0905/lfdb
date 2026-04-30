use std::{
  fs::{remove_file, rename, File, OpenOptions},
  io::IoSlice,
  mem::transmute,
  path::{Path, PathBuf},
  sync::{Arc, Mutex},
};

use super::{SegmentGeneration, WAL_BLOCK_SIZE};
use crate::{
  disk::{max_iov, DirectIO, Fallocate, Page, Pointer, Pread, Pwrite, Pwritev},
  error::Result,
  thread::{BackgroundThread, TaskHandle, WorkBuilder},
  utils::{ShortenedMutex, ToArc, ToBox},
  Error,
};

pub const FILE_EXT: &str = "wal";

pub type FsyncResult = TaskHandle<Result>;

const SIZE: Pointer = WAL_BLOCK_SIZE as Pointer;

pub struct WALSegment {
  file: Arc<File>,
  path: Mutex<PathBuf>,
  io: Box<dyn BackgroundThread<(Pointer, &'static [u8]), Result>>,
  flush: Box<dyn BackgroundThread<(), Result>>,
}
impl WALSegment {
  pub fn parse_generation(path: &Path) -> Result<SegmentGeneration> {
    let generation = path
      .file_stem()
      .unwrap()
      .to_string_lossy()
      .parse()
      .map_err(Error::unknown)?;
    Ok(generation)
  }

  pub fn open_new(
    prefix: &Path,
    generation: SegmentGeneration,
    flush_count: usize,
    max_len: Pointer,
  ) -> Result<Self> {
    let path = prefix.join(pad_start(generation)).with_extension(FILE_EXT);

    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .direct_io(&path)
      .map_err(Error::IO)?
      .to_arc();

    // Pre-allocate the full file space upfront. Segments are rarely created fresh —
    // they are almost always reused via rename(). Paying the allocation cost once
    // at creation avoids metadata updates on every subsequent write.
    let file_len = max_len * SIZE;
    file
      .fallocate(0, file_len)
      .and_then(|_| file.set_len(file_len))
      .and_then(|_| file.sync_all()) // sync metadata for replay at once
      .map_err(Error::IO)?;
    Ok(Self::new(file, path, flush_count))
  }
  pub fn open_exists(path: &Path, flush_count: usize) -> Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .direct_io(path)
      .map_err(Error::IO)?
      .to_arc();
    Ok(Self::new(file, path.to_path_buf(), flush_count))
  }

  pub fn read(&self, pointer: Pointer, page: &mut Page<WAL_BLOCK_SIZE>) -> Result {
    self
      .file
      .pread(page.as_mut(), pointer * SIZE)
      .map(|_| ())
      .map_err(Error::IO)
  }
  pub fn write(&self, pointer: Pointer, page: &Page<WAL_BLOCK_SIZE>) -> Result {
    // transmute extends the slice lifetime to 'static to satisfy the background thread's
    // type bound. Safe because wait and flatten blocks until the write completes, ensuring
    // the page buffer outlives the background thread's use of the pointer.
    self
      .io
      .execute((pointer, unsafe { transmute(page.as_ref()) }))
      .wait()
      .flatten()
  }
  #[inline]
  pub fn len(&self) -> Result<Pointer> {
    let metadata = self.file.metadata().map_err(Error::IO)?;
    Ok(metadata.len().div_ceil(SIZE))
  }

  /**
   * Repurposes this segment for a new generation by renaming it in place.
   * Much faster than creating a new file — avoids the fallocate + metadata sync cost.
   */
  pub fn reuse(&self, prefix: &Path, generation: SegmentGeneration) -> Result {
    let new_path = prefix.join(pad_start(generation)).with_extension(FILE_EXT);
    let mut path = self.path.l();
    rename(path.as_path(), &new_path).map_err(Error::IO)?;
    *path = new_path;
    Ok(())
  }

  fn new(file: Arc<File>, path: PathBuf, flush_count: usize) -> Self {
    let io = WorkBuilder::new()
      .name(format!("{:?} buffered write", path.as_path().as_os_str()))
      .single()
      .eager_buffering(max_iov(), handle_write(file.clone()))
      .to_box();

    let flush = WorkBuilder::new()
      .name(format!("{} flush", path.as_path().to_string_lossy()))
      .single()
      .eager_buffering(flush_count, handle_flush(file.clone()))
      .to_box();
    Self {
      file,
      io,
      flush,
      path: Mutex::new(path),
    }
  }

  #[inline]
  pub fn fsync(&self) -> FsyncResult {
    self.flush.execute(())
  }

  #[inline]
  pub fn truncate(self) -> Result {
    self.close();
    remove_file(self.path.l().as_path()).map_err(Error::IO)?;
    Ok(())
  }

  #[inline]
  pub fn close(&self) {
    self.io.close();
    self.flush.close();
  }
}

#[inline]
const fn handle_flush(file: Arc<File>) -> impl Fn(Vec<()>) -> Result {
  move |_| file.sync_data().map_err(Error::IO)
}

/**
 * Zero-pad to 20 digits: ensures lexicographic file ordering matches numeric order,
 * and accommodates the full u64 range (max 20 digits).
 */
fn pad_start(n: SegmentGeneration) -> String {
  format!("{:0>20}", n)
}

const fn handle_write(file: Arc<File>) -> impl FnMut(Vec<(Pointer, &[u8])>) -> Result {
  move |mut buffered| {
    if buffered.len() == 1 {
      let (i, slice) = buffered[0];
      return file.pwrite(slice, i * SIZE).map_err(Error::IO).map(drop);
    }

    // Duplicate offsets all point to the same underlying page memory (same PageRef),
    // so writing once is equivalent — no data is lost by deduplicating.
    buffered.sort_by_key(|(i, _)| *i);
    buffered.dedup_by_key(|(i, _)| *i);

    buffered
      .chunk_by(|(a, _), (b, _)| *a + 1 == *b)
      .map(|g| g.into_iter())
      .map(|g| g.map(|(i, s)| (*i, IoSlice::new(*s))).unzip())
      .map(|(ptrs, bufs): (Vec<_>, Vec<_>)| (ptrs[0] * SIZE, bufs))
      .map(|(offset, bufs)| file.pwritev(&bufs, offset))
      .fold(Ok(()), |a, c| a.and_then(|_| c.map(drop)))
      .map_err(Error::IO)
  }
}
