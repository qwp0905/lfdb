use std::{
  cell::Cell,
  fs::{remove_file, File, OpenOptions},
  io::{Error as IoError, ErrorKind, IoSlice},
  mem::forget,
  panic::RefUnwindSafe,
  path::Path,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
};

use crossbeam::{queue::SegQueue, utils::Backoff};

use super::{max_iov, DirectIO, Fallocate, Page, Pointer, Pread, Pwrite, Pwritev};
use crate::{
  error::{Error, Result},
  metrics::MetricsRegistry,
  thread::{oneshot, BackgroundThread, OneshotFulfill, TaskHandle, WorkBuilder},
  utils::{ExclusivePin, ToArc},
};

const EXTENT: Pointer = 64;

type ThreadArg<const N: usize> = (Arc<File>, Arc<WriteHandle<N>>);
type WriteThread<const N: usize> = dyn BackgroundThread<ThreadArg<N>, ()>;
type WriteTask<const N: usize> = (Pointer, &'static Page<N>);
type WriteQueue<const N: usize> = SegQueue<(WriteTask<N>, OneshotFulfill<Result>)>;

struct WriteHandle<const N: usize> {
  queue: WriteQueue<N>,
  occupied: AtomicBool,
  thread: Arc<WriteThread<N>>,
  /**
   * Pin to protect file I/O
   */
  pin: ExclusivePin,
  /**
   * Flag to check for file existence a little faster.
   */
  closed: AtomicBool,
  allocated: Cell<Pointer>,
}
impl<const N: usize> WriteHandle<N> {
  fn new(thread: Arc<WriteThread<N>>, len: Pointer) -> Self {
    Self {
      queue: SegQueue::new(),
      occupied: AtomicBool::new(false),
      thread,
      closed: AtomicBool::new(false),
      pin: ExclusivePin::new(),
      allocated: Cell::new(len),
    }
  }

  fn execute(
    self: &Arc<Self>,
    file: &Arc<File>,
    pointer: Pointer,
    page: &'static Page<N>,
  ) -> TaskHandle<()> {
    let (o, f) = oneshot();
    let handle = TaskHandle::new(o);
    if self.closed.load(Ordering::Acquire) {
      f.fulfill(Ok(()));
      return handle;
    }

    self.queue.push(((pointer, page), f));
    if self.occupied.fetch_or(true, Ordering::Release) {
      return handle;
    }

    self.thread.dispatch((file.clone(), self.clone()));
    handle
  }
}
unsafe impl<const N: usize> Send for WriteHandle<N> {}
unsafe impl<const N: usize> Sync for WriteHandle<N> {}
impl<const N: usize> RefUnwindSafe for WriteHandle<N> {}

pub struct IOPool<const N: usize> {
  thread: Arc<WriteThread<N>>,
  metrics: Arc<MetricsRegistry>,
}
impl<const N: usize> IOPool<N> {
  const SIZE: Pointer = N as Pointer;
  const EXTENT_SIZE: Pointer = EXTENT * Self::SIZE;

  pub fn new(thread_count: usize, metrics: Arc<MetricsRegistry>) -> Self {
    let thread = WorkBuilder::new()
      .name("io pool")
      .multi(thread_count)
      .shared(Self::handle_thread(metrics.clone()))
      .to_arc();
    Self { thread, metrics }
  }

  #[inline]
  pub fn open_controller(&self, path: &Path) -> Result<DiskController<N>> {
    // Direct IO bypasses the OS page cache for predictable latency.
    // To compensate for the lack of OS write buffering, writes are
    // accumulated and sorted in the eager_buffering layer, then
    // flushed as a single pwritev call per contiguous block.
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .direct_io(path)
      .map_err(Error::IO)?
      .to_arc();
    let len = file.metadata().map_err(Error::IO)?.len() / Self::SIZE;
    Ok(DiskController::new(
      file,
      WriteHandle::new(self.thread.clone(), len).to_arc(),
      self.metrics.clone(),
    ))
  }

  fn write(
    metrics: &MetricsRegistry,
    file: &File,
    allocated: &Cell<Pointer>,
    mut buffered: Vec<WriteTask<N>>,
  ) -> Result {
    if buffered.len() == 1 {
      let (p, slice) = &buffered[0];
      while allocated.get() <= *p {
        file
          .fallocate(allocated.get() * Self::SIZE, Self::EXTENT_SIZE)
          .map_err(Error::IO)?;
        allocated.set(allocated.get() + EXTENT);
      }
      return metrics
        .disk_write
        .measure(|| file.pwrite_all(slice.as_ref(), p * Self::SIZE))
        .map(|_| ())
        .map_err(Error::IO);
    }

    // last caller wins on duplicate pointers
    buffered.sort_by_key(|(i, _)| *i);
    buffered.reverse();
    buffered.dedup_by_key(|(i, _)| *i);
    buffered.reverse();

    let p = buffered.last().unwrap().0;
    while allocated.get() <= p {
      file
        .fallocate(allocated.get() * Self::SIZE, Self::EXTENT_SIZE)
        .map_err(Error::IO)?;
      allocated.set(allocated.get() + EXTENT);
    }
    buffered
      .chunk_by(|(a, _), (b, _)| *a + 1 == *b)
      .map(|g| g.into_iter())
      .map(|g| g.map(|(p, s)| (*p, IoSlice::new(s.as_ref().as_ref()))))
      .map(|g| g.unzip())
      .map(|(ptrs, bufs): (Vec<_>, Vec<_>)| ((ptrs[0] * Self::SIZE), bufs))
      .map(|(offset, mut bufs)| move || file.pwritev_all(&mut bufs, offset))
      .map(|closure| metrics.disk_write.measure(closure))
      .map(|r| r.map_err(Error::IO))
      .collect()
  }

  fn flush(
    metrics: &MetricsRegistry,
    file: &File,
    pin: &ExclusivePin,
    closed: &AtomicBool,
    allocated: &Cell<Pointer>,
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

    let result = Self::write(metrics, file, allocated, values);
    waiting
      .into_iter()
      .for_each(|done| done.fulfill(result.clone()));
  }

  const fn handle_thread(metrics: Arc<MetricsRegistry>) -> impl Fn(ThreadArg<N>) {
    let count = max_iov();
    move |(file, handle)| {
      metrics.active_io_threads.inc();

      let mut buffered = Vec::with_capacity(count);
      loop {
        for task in (0..count).map_while(|_| handle.queue.pop()) {
          buffered.push(task);
        }

        Self::flush(
          &metrics,
          &file,
          &handle.pin,
          &handle.closed,
          &handle.allocated,
          &mut buffered,
        );
        handle.occupied.fetch_and(false, Ordering::Release);
        if handle.queue.is_empty() {
          break;
        }
        if handle.occupied.fetch_or(true, Ordering::AcqRel) {
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
  write_handle: Arc<WriteHandle<N>>,
  metrics: Arc<MetricsRegistry>,
}
impl<const N: usize> DiskController<N> {
  const SIZE: Pointer = N as Pointer;

  fn new(
    file: Arc<File>,
    write_handle: Arc<WriteHandle<N>>,
    metrics: Arc<MetricsRegistry>,
  ) -> Self {
    Self {
      file,
      write_handle,
      metrics,
    }
  }

  pub fn read<'a>(&self, pointer: Pointer, page: &'a mut Page<N>) -> Result {
    self
      .metrics
      .disk_read
      .measure(|| self.file.pread_exact(page.as_mut(), pointer * Self::SIZE))
      .map_err(Error::IO)
  }

  pub fn read_unchecked<'a>(&self, pointer: Pointer, page: &'a mut Page<N>) -> Result {
    let mut offset = pointer * Self::SIZE;
    let mut buf = page.as_mut();
    while !buf.is_empty() {
      match self.file.pread(buf, offset) {
        Ok(0) if buf.len() == N => break, // allow only empty, not partial.
        Ok(0) => return Err(Error::IO(IoError::from(ErrorKind::UnexpectedEof))),
        Ok(n) => {
          let tmp = buf;
          buf = &mut tmp[n..];
          offset += n as u64;
        }
        Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
        Err(e) => return Err(Error::IO(e)),
      }
    }
    Ok(())
  }

  #[inline]
  pub fn write_async(&self, pointer: Pointer, page: &'static Page<N>) -> TaskHandle<()> {
    self.write_handle.execute(&self.file, pointer, page)
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

  pub fn truncate(&self, path: &Path) -> Result {
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
