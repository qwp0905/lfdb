use std::{
  fs::{File, OpenOptions},
  io::IoSlice,
  path::PathBuf,
  sync::Arc,
};

use super::{max_iov, DirectIO, PageRef, Pread, Pwrite, Pwritev};
use crate::{
  disk::PagePool,
  error::{Error, Result},
  metrics::MetricsRegistry,
  thread::{BackgroundThread, WorkBuilder, WorkResult},
  utils::{ToArc, ToBox},
};

fn handle_write<const N: usize>(
  file: Arc<File>,
  metrics: Arc<MetricsRegistry>,
) -> impl FnMut(Vec<(usize, PageRef<N>)>) -> Result {
  move |mut buffered| {
    if buffered.len() == 1 {
      let (i, slice) = &buffered[0];
      return file
        .pwrite(slice.as_ref().as_ref(), (i * N) as u64)
        .map_err(Error::IO)
        .map(drop);
    }

    buffered.sort_by_key(|(i, _)| *i);
    buffered
      .chunk_by(|(a, _), (b, _)| *a + 1 == *b)
      .map(|g| g.into_iter())
      .map(|g| g.map(|(i, s)| (*i, IoSlice::new(s.as_ref().as_ref()))))
      .map(|g| g.unzip())
      .map(|(indexes, bufs): (Vec<_>, Vec<_>)| ((indexes[0] * N), bufs))
      .map(|(offset, bufs)| {
        metrics
          .disk_write
          .measure(|| file.pwritev(&bufs, offset as u64))
      })
      .map(|r| r.map(drop).map_err(Error::IO))
      .collect()
  }
}

pub struct WriteAsync<const N: usize>(WorkResult<Result>);
impl<const N: usize> WriteAsync<N> {
  pub fn wait(self) -> Result {
    self.0.wait()?
  }
}

/**
 * Provides block-level IO to a single data file.
 * Write requests are buffered and batched into pwritev syscalls
 * via a background thread for efficient sequential disk access.
 */
pub struct DiskController<const N: usize> {
  file: Arc<File>,
  writer: Box<dyn BackgroundThread<(usize, PageRef<N>), Result>>,
  page_pool: Arc<PagePool<N>>,
  metrics: Arc<MetricsRegistry>,
}
impl<const N: usize> DiskController<N> {
  pub fn open(
    path: PathBuf,
    page_pool: Arc<PagePool<N>>,
    metrics: Arc<MetricsRegistry>,
  ) -> Result<Self> {
    // Direct IO bypasses the OS page cache for predictable latency.
    // To compensate for the lack of OS write buffering, writes are
    // accumulated and sorted in the eager_buffering layer, then
    // flushed as a single pwritev call per contiguous block.
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .direct_io(&path)
      .map_err(Error::IO)?
      .to_arc();
    let writer = WorkBuilder::new()
      .name(format!("{} write buffering", path.to_string_lossy()))
      .stack_size(2 << 20)
      .single()
      .eager_buffering(max_iov(), handle_write::<N>(file.clone(), metrics.clone()))
      .to_box();

    Ok(Self {
      file,
      writer,
      page_pool,
      metrics,
    })
  }
  pub fn read<'a>(&self, index: usize, page: &'a mut PageRef<N>) -> Result {
    self
      .metrics
      .disk_read
      .measure(|| self.file.pread(page.as_mut().as_mut(), (index * N) as u64))
      .map(|_| ())
      .map_err(Error::IO)
  }
  pub fn write_async<'a>(&self, index: usize, page: &'a PageRef<N>) -> WriteAsync<N> {
    // Copy the page into a pooled buffer before sending to the writer thread.
    // The original PageRef is held under a lock, which cannot be transferred
    // across thread boundaries. Copying also releases the lock immediately,
    // allowing concurrent access while IO happens in the background.
    let mut pooled = self.page_pool.acquire();
    pooled.as_mut().copy_from(page.as_ref());
    WriteAsync(self.writer.send((index, pooled)))
  }
  pub fn write<'a>(&self, index: usize, page: &'a PageRef<N>) -> Result {
    self.write_async(index, page).wait()
  }

  pub fn fsync(&self) -> Result {
    self.file.sync_all().map_err(Error::IO)
  }

  pub fn len(&self) -> Result<usize> {
    let meta = self.file.metadata().map_err(Error::IO)?;
    Ok((meta.len() as usize) / N)
  }

  pub fn close(&self) {
    self.writer.close();
  }
}
