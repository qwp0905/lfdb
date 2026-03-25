use std::{
  fs::{remove_file, rename, File, OpenOptions},
  io::IoSlice,
  mem::transmute,
  path::{Path, PathBuf},
  sync::{Arc, Mutex},
};

use super::WAL_BLOCK_SIZE;
use crate::{
  constant::FILE_SUFFIX,
  disk::{max_iov, DirectIO, Fallocate, Page, Pread, Pwrite, Pwritev},
  error::Result,
  thread::{BackgroundThread, WorkBuilder, WorkResult},
  utils::{ShortenedMutex, ToArc, ToBox},
  Error,
};

pub struct FsyncResult(WorkResult<bool>);
impl FsyncResult {
  pub fn wait(self) -> Result {
    self
      .0
      .wait()?
      .then(|| Ok(()))
      .unwrap_or_else(|| Err(Error::FlushFailed))
  }
}

pub struct WALSegment {
  file: Arc<File>,
  path: Mutex<PathBuf>,
  io: Box<dyn BackgroundThread<(usize, &'static [u8]), Result>>,
  flush: Box<dyn BackgroundThread<(), bool>>,
}
impl WALSegment {
  pub fn parse_generation<A, B>(filename: &A, prefix: &B) -> Result<usize>
  where
    A: AsRef<str>,
    B: AsRef<str>,
  {
    let generation: usize = filename
      .as_ref()
      .replace(prefix.as_ref(), "")
      .trim_end_matches(FILE_SUFFIX)
      .parse()
      .map_err(Error::unknown)?;
    Ok(generation)
  }

  pub fn open_new<P: AsRef<Path>>(
    prefix: P,
    generation: usize,
    flush_count: usize,
    max_len: usize,
  ) -> Result<Self> {
    let path = format!(
      "{}{}{}",
      prefix.as_ref().to_string_lossy(),
      pad_start(generation),
      FILE_SUFFIX
    );

    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .direct_io(&path)
      .map_err(Error::IO)?
      .to_arc();

    let file_len = (WAL_BLOCK_SIZE * max_len) as u64;
    file
      .fallocate(0, file_len)
      .and_then(|_| file.set_len(file_len))
      .and_then(|_| file.sync_all()) // sync metadata for replay at once
      .map_err(Error::IO)?;
    Ok(Self::new(file, path.into(), flush_count))
  }
  pub fn open_exists<P: AsRef<Path>>(path: P, flush_count: usize) -> Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .direct_io(path.as_ref())
      .map_err(Error::IO)?
      .to_arc();
    Ok(Self::new(file, path.as_ref().into(), flush_count))
  }

  pub fn read<P: AsMut<Page<WAL_BLOCK_SIZE>>>(
    &self,
    index: usize,
    page: &mut P,
  ) -> Result {
    self
      .file
      .pread(page.as_mut().as_mut(), (index * WAL_BLOCK_SIZE) as u64)
      .map(|_| ())
      .map_err(Error::IO)
  }
  pub fn write<P: AsRef<Page<WAL_BLOCK_SIZE>>>(&self, index: usize, page: &P) -> Result {
    self
      .io
      .send((index, unsafe { transmute(page.as_ref().as_ref()) }))
      .wait_flatten()
  }
  pub fn len(&self) -> Result<usize> {
    let metadata = self.file.metadata().map_err(Error::IO)?;
    Ok((metadata.len() as usize).div_ceil(WAL_BLOCK_SIZE))
  }

  pub fn reuse<P: AsRef<Path>>(&self, prefix: P, generation: usize) -> Result {
    let new_path = format!(
      "{}{}{}",
      prefix.as_ref().to_string_lossy(),
      pad_start(generation),
      FILE_SUFFIX
    );
    let mut path = self.path.l();
    rename(path.as_path(), &new_path).map_err(Error::IO)?;
    *path = PathBuf::from(new_path);
    Ok(())
  }

  fn new(file: Arc<File>, path: PathBuf, flush_count: usize) -> Self {
    let io = WorkBuilder::new()
      .name(format!(
        "{} buffered write",
        path.as_path().to_string_lossy()
      ))
      .stack_size(2 << 20)
      .single()
      .eager_buffering(max_iov(), handle_write(file.clone()))
      .to_box();

    let flush = WorkBuilder::new()
      .name(format!("{} flush", path.as_path().to_string_lossy()))
      .stack_size(2 << 20)
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

  pub fn fsync(&self) -> FsyncResult {
    FsyncResult(self.flush.send(()))
  }

  pub fn truncate(self) -> Result {
    self.flush.close();
    remove_file(self.path.l().as_path()).map_err(Error::IO)?;
    Ok(())
  }

  pub fn close(&self) {
    self.flush.close();
  }
}

fn handle_flush(file: Arc<File>) -> impl Fn(Vec<()>) -> bool {
  move |_| file.sync_data().is_ok()
}

fn pad_start(n: usize) -> String {
  format!("{:0>20}", n)
}

fn handle_write(file: Arc<File>) -> impl FnMut(Vec<(usize, &[u8])>) -> Result {
  move |mut buffered| {
    if buffered.len() == 1 {
      let (i, slice) = buffered[0];
      return file
        .pwrite(slice, (i * WAL_BLOCK_SIZE) as u64)
        .map_err(Error::IO)
        .map(drop);
    }

    buffered.dedup_by_key(|(i, _)| *i);
    buffered.sort_by_key(|(i, _)| *i);

    buffered
      .chunk_by(|(a, _), (b, _)| *a + 1 == *b)
      .map(|g| g.into_iter())
      .map(|g| g.map(|(i, s)| (*i, IoSlice::new(*s))).unzip())
      .map(|(indexes, bufs): (Vec<_>, Vec<_>)| ((indexes[0] * WAL_BLOCK_SIZE), bufs))
      .map(|(offset, bufs)| file.pwritev(&bufs, offset as u64))
      .fold(Ok(()), |a, c| a.and_then(|_| c.map(drop)))
      .map_err(Error::IO)
  }
}
