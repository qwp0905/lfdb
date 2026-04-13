use std::{
  fs::{File, OpenOptions},
  io::IoSlice,
  path::Path,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
};

use crossbeam::queue::SegQueue;

use super::{max_iov, DirectIO, PagePool, PageRef, Pointer, Pread, Pwrite, Pwritev};
use crate::{
  error::{Error, Result},
  metrics::MetricsRegistry,
  thread::{oneshot, BackgroundThread, OneshotFulfill, TaskHandle, WorkBuilder},
  utils::ToArc,
};

type WriteTask<const N: usize> = (Pointer, PageRef<N>);
type WriteQueue<const N: usize> = SegQueue<(WriteTask<N>, OneshotFulfill<Result>)>;

struct WriteHandle<const N: usize> {
  queue: Arc<WriteQueue<N>>,
  occupied: Arc<AtomicBool>,
  page_pool: Arc<PagePool<N>>,
  thread: Arc<dyn BackgroundThread<(Arc<File>, Arc<WriteQueue<N>>, Arc<AtomicBool>), ()>>,
}
impl<const N: usize> WriteHandle<N> {
  fn new(
    page_pool: Arc<PagePool<N>>,
    thread: Arc<
      dyn BackgroundThread<(Arc<File>, Arc<WriteQueue<N>>, Arc<AtomicBool>), ()>,
    >,
  ) -> Self {
    Self {
      queue: SegQueue::new().to_arc(),
      occupied: AtomicBool::new(false).to_arc(),
      page_pool,
      thread,
    }
  }

  fn execute<'a>(
    &self,
    file: &Arc<File>,
    pointer: Pointer,
    page: &'a PageRef<N>,
  ) -> TaskHandle<()> {
    // Copy the page into a pooled buffer before sending to the writer thread.
    // The original PageRef is held under a lock, which cannot be transferred
    // across thread boundaries. Copying also releases the lock immediately,
    // allowing concurrent access while IO happens in the background.

    let mut pooled = self.page_pool.acquire();
    pooled.as_mut().copy_from(page.as_ref());

    let (o, f) = oneshot();
    let handle = TaskHandle::from(o);
    self.queue.push(((pointer, pooled), f));
    if self.occupied.fetch_or(true, Ordering::Release) {
      return handle;
    }

    self
      .thread
      .dispatch((file.clone(), self.queue.clone(), self.occupied.clone()));
    handle
  }
}

pub struct IOPool<const N: usize> {
  thread: Arc<dyn BackgroundThread<(Arc<File>, Arc<WriteQueue<N>>, Arc<AtomicBool>), ()>>,
  page_pool: Arc<PagePool<N>>,
  metrics: Arc<MetricsRegistry>,
}
impl<const N: usize> IOPool<N> {
  const SIZE: Pointer = N as Pointer;

  pub fn new(
    thread_count: usize,
    page_pool: Arc<PagePool<N>>,
    metrics: Arc<MetricsRegistry>,
  ) -> Self {
    let thread = WorkBuilder::new()
      .name("io pool")
      .multi(thread_count)
      .shared(Self::handle_thread(metrics.clone()))
      .to_arc();
    Self {
      thread,
      page_pool,
      metrics,
    }
  }

  #[inline]
  pub fn open_controller<P: AsRef<Path>>(&self, path: P) -> Result<DiskController<N>> {
    DiskController::open(
      path,
      WriteHandle::new(self.page_pool.clone(), self.thread.clone()),
      self.metrics.clone(),
    )
  }

  fn write(
    metrics: &MetricsRegistry,
    file: &File,
    mut buffered: Vec<WriteTask<N>>,
  ) -> Result {
    if buffered.len() == 1 {
      let (p, slice) = &buffered[0];
      return metrics
        .disk_write
        .measure(|| file.pwrite(slice.as_ref().as_ref(), p * Self::SIZE))
        .map_err(Error::IO)
        .map(drop);
    }

    // last caller wins on duplicate pointers
    buffered.sort_by_key(|(i, _)| *i);
    buffered.reverse();
    buffered.dedup_by_key(|(i, _)| *i);
    buffered.reverse();

    buffered
      .chunk_by(|(a, _), (b, _)| *a + 1 == *b)
      .map(|g| g.into_iter())
      .map(|g| g.map(|(p, s)| (*p, IoSlice::new(s.as_ref().as_ref()))))
      .map(|g| g.unzip())
      .map(|(ptrs, bufs): (Vec<_>, Vec<_>)| ((ptrs[0] * Self::SIZE), bufs))
      .map(|(offset, bufs)| metrics.disk_write.measure(|| file.pwritev(&bufs, offset)))
      .map(|r| r.map(drop).map_err(Error::IO))
      .collect()
  }

  fn flush(
    metrics: &MetricsRegistry,
    file: &File,
    buffered: &mut Vec<(WriteTask<N>, OneshotFulfill<Result>)>,
  ) {
    if buffered.is_empty() {
      return;
    }

    let (values, waiting): (Vec<_>, Vec<_>) = buffered.drain(..).unzip();
    let result = Self::write(metrics, file, values)
      .map(Ok)
      .unwrap_or_else(Err);
    waiting
      .into_iter()
      .for_each(|done| done.fulfill(result.clone()));
  }

  fn handle_thread(
    metrics: Arc<MetricsRegistry>,
  ) -> impl Fn((Arc<File>, Arc<WriteQueue<N>>, Arc<AtomicBool>)) {
    let count = max_iov();
    move |(file, queue, occupied)| {
      metrics.active_io_threads.inc();

      let mut buffered = Vec::with_capacity(count);

      loop {
        'inner: while buffered.len() < count {
          match queue.pop() {
            Some(task) => buffered.push(task),
            None => break 'inner,
          }
        }

        Self::flush(&metrics, &file, &mut buffered);
        occupied.fetch_and(false, Ordering::Release);
        if queue.is_empty() {
          break;
        }
        if occupied.fetch_or(true, Ordering::AcqRel) {
          break;
        }
      }

      metrics.active_io_threads.dec();
    }
  }

  pub fn close(&self) {
    self.thread.close();
  }
}

/**
 * Provides block-level IO to a single data file.
 * Write requests are buffered and batched into pwritev syscalls
 * via a background thread for efficient sequential disk access.
 */
pub struct DiskController<const N: usize> {
  file: Arc<File>,
  write_handle: WriteHandle<N>,
  metrics: Arc<MetricsRegistry>,
}
impl<const N: usize> DiskController<N> {
  const SIZE: Pointer = N as Pointer;

  fn open<P: AsRef<Path>>(
    path: P,
    write_handle: WriteHandle<N>,
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
      .direct_io(path.as_ref())
      .map_err(Error::IO)?
      .to_arc();

    Ok(Self {
      file,
      write_handle,
      metrics,
    })
  }

  pub fn read<'a>(&self, pointer: Pointer, page: &'a mut PageRef<N>) -> Result {
    self
      .metrics
      .disk_read
      .measure(|| {
        self
          .file
          .pread(page.as_mut().as_mut(), pointer * Self::SIZE)
      })
      .map(|_| ())
      .map_err(Error::IO)
  }

  #[inline]
  pub fn write_async<'a>(
    &self,
    pointer: Pointer,
    page: &'a PageRef<N>,
  ) -> TaskHandle<()> {
    self.write_handle.execute(&self.file, pointer, page)
  }
  #[inline]
  pub fn write<'a>(&self, pointer: Pointer, page: &'a PageRef<N>) -> Result {
    self.write_async(pointer, page).wait()
  }

  #[inline]
  pub fn fsync(&self) -> Result {
    self.file.sync_all().map_err(Error::IO)
  }

  #[inline]
  pub fn len(&self) -> Result<Pointer> {
    let meta = self.file.metadata().map_err(Error::IO)?;
    Ok(meta.len() / Self::SIZE)
  }
}
