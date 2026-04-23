use std::{
  fs::{remove_file, File, OpenOptions},
  io::IoSlice,
  mem::forget,
  path::Path,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
};

use crossbeam::{queue::SegQueue, utils::Backoff};

use super::{max_iov, DirectIO, PageRef, Pointer, Pread, Pwrite, Pwritev};
use crate::{
  error::{Error, Result},
  metrics::MetricsRegistry,
  thread::{oneshot, BackgroundThread, OneshotFulfill, TaskHandle, WorkBuilder},
  utils::{ExclusivePin, ToArc},
};

type ThreadArg<const N: usize> = (
  Arc<File>,
  Arc<WriteQueue<N>>,
  Arc<AtomicBool>,
  Arc<ExclusivePin>,
  Arc<AtomicBool>,
);
type WriteThread<const N: usize> = dyn BackgroundThread<ThreadArg<N>, ()>;
type WriteTask<const N: usize> = (Pointer, &'static PageRef<N>);
type WriteQueue<const N: usize> = SegQueue<(WriteTask<N>, OneshotFulfill<Result>)>;

struct WriteHandle<const N: usize> {
  queue: Arc<WriteQueue<N>>,
  occupied: Arc<AtomicBool>,
  thread: Arc<WriteThread<N>>,
  /**
   * Pin to protect file I/O
   */
  pin: Arc<ExclusivePin>,
  /**
   * Flag to check for file existence a little faster.
   */
  closed: Arc<AtomicBool>,
}
impl<const N: usize> WriteHandle<N> {
  fn new(thread: Arc<WriteThread<N>>) -> Self {
    Self {
      queue: SegQueue::new().to_arc(),
      occupied: AtomicBool::new(false).to_arc(),
      thread,
      closed: AtomicBool::new(false).to_arc(),
      pin: ExclusivePin::new().to_arc(),
    }
  }

  fn execute(
    &self,
    file: &Arc<File>,
    pointer: Pointer,
    page: &'static PageRef<N>,
  ) -> TaskHandle<()> {
    let (o, f) = oneshot();
    let handle = TaskHandle::from(o);
    if self.closed.load(Ordering::Acquire) {
      f.fulfill(Ok(()));
      return handle;
    }

    self.queue.push(((pointer, page), f));
    if self.occupied.fetch_or(true, Ordering::Release) {
      return handle;
    }

    self.thread.dispatch((
      file.clone(),
      self.queue.clone(),
      self.occupied.clone(),
      self.pin.clone(),
      self.closed.clone(),
    ));
    handle
  }
}

pub struct IOPool<const N: usize> {
  thread: Arc<WriteThread<N>>,
  metrics: Arc<MetricsRegistry>,
}
impl<const N: usize> IOPool<N> {
  const SIZE: Pointer = N as Pointer;

  pub fn new(thread_count: usize, metrics: Arc<MetricsRegistry>) -> Self {
    let thread = WorkBuilder::new()
      .name("io pool")
      .multi(thread_count)
      .shared(Self::handle_thread(metrics.clone()))
      .to_arc();
    Self { thread, metrics }
  }

  #[inline]
  pub fn open_controller<P: AsRef<Path>>(&self, path: P) -> Result<DiskController<N>> {
    DiskController::open(
      path,
      WriteHandle::new(self.thread.clone()),
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
    pin: &ExclusivePin,
    closed: &AtomicBool,
    buffered: &mut Vec<(WriteTask<N>, OneshotFulfill<Result>)>,
  ) {
    if buffered.is_empty() {
      return;
    }

    let (values, waiting): (Vec<_>, Vec<_>) = buffered.drain(..).unzip();
    let _token = match pin.try_shared() {
      Some(t) => t,
      None => {
        closed.fetch_or(true, Ordering::Release);
        return waiting.into_iter().for_each(|done| done.fulfill(Ok(())));
      }
    };

    let result = Self::write(metrics, file, values)
      .map(Ok)
      .unwrap_or_else(Err);
    waiting
      .into_iter()
      .for_each(|done| done.fulfill(result.clone()));
  }

  fn handle_thread(metrics: Arc<MetricsRegistry>) -> impl Fn(ThreadArg<N>) {
    let count = max_iov();
    move |(file, queue, occupied, pin, closed)| {
      metrics.active_io_threads.inc();

      let mut buffered = Vec::with_capacity(count);

      loop {
        'inner: while buffered.len() < count {
          match queue.pop() {
            Some(task) => buffered.push(task),
            None => break 'inner,
          }
        }

        Self::flush(&metrics, &file, &pin, &closed, &mut buffered);
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
  pub fn write_async(
    &self,
    pointer: Pointer,
    page: &'static PageRef<N>,
  ) -> TaskHandle<()> {
    self.write_handle.execute(&self.file, pointer, page)
  }
  #[inline]
  pub fn write(&self, pointer: Pointer, page: &'static PageRef<N>) -> Result {
    self.write_async(pointer, page).wait()
  }

  #[inline]
  pub fn fsync(&self) -> Result {
    let _token = match self.write_handle.pin.try_shared() {
      Some(token) => token,
      None => return Ok(()),
    };

    self.file.sync_all().map_err(Error::IO)
  }

  #[inline]
  pub fn len(&self) -> Result<Pointer> {
    let meta = self.file.metadata().map_err(Error::IO)?;
    Ok(meta.len() / Self::SIZE)
  }

  pub fn truncate<P: AsRef<Path>>(&self, path: P) -> Result {
    let backoff = Backoff::new();
    loop {
      match self.write_handle.pin.try_exclusive() {
        Some(t) => break forget(t),
        None => backoff.snooze(),
      }
    }

    remove_file(path).map_err(Error::IO)
  }
}
