use std::{
  collections::VecDeque,
  fs::Metadata,
  io::{Error, ErrorKind},
  sync::{mpsc, Mutex},
  thread,
  time::Duration,
};

use super::*;
use crate::disk::TaskType;

const BLOCK: usize = 4096;
const EXTENT: u64 = 1 << 20;
const TIMEOUT: Duration = Duration::from_secs(5);

/**
 * Submitted tasks are held until the test completes them, so requests published
 * in the meantime deterministically land in the next batch.
 */
#[derive(Default)]
struct HeldBackend(Mutex<VecDeque<IOTask>>);
impl HeldBackend {
  fn take(&self) -> IOTask {
    self
      .0
      .lock()
      .unwrap()
      .pop_front()
      .expect("no submitted task")
  }
  fn is_idle(&self) -> bool {
    self.0.lock().unwrap().is_empty()
  }
}
impl IOBackend for HeldBackend {
  fn submit(&self, task: IOTask) {
    self.0.lock().unwrap().push_back(task);
  }
  fn pread(&self, _: &mut [u8], _: u64) -> Result<usize> {
    unreachable!()
  }
  fn metadata(&self) -> Result<Metadata> {
    unreachable!()
  }
  fn try_flock(&self) -> Result<bool> {
    unreachable!()
  }
  fn unlock(&self) -> Result<()> {
    unreachable!()
  }
}

struct Fixture {
  backend: Arc<HeldBackend>,
  scheduler: Arc<IOScheduler>,
  metrics: Arc<MetricsRegistry>,
}
impl Fixture {
  fn new(allocated: Option<u64>) -> Self {
    let backend = Arc::new(HeldBackend::default());
    let scheduler = IOScheduler::new(backend.clone(), allocated.map(AllocState::new));
    Self {
      backend,
      scheduler: Arc::new(scheduler),
      metrics: Arc::new(MetricsRegistry::new()),
    }
  }

  fn write(&self, offset: u64, data: &'static [u8]) -> Oneshot<Result<()>> {
    let pin = self.scheduler.try_pin_owned().unwrap();
    self
      .scheduler
      .publish_write(data, offset, pin, &self.metrics)
  }

  fn sync(&self) -> Oneshot<Result<()>> {
    let pin = self.scheduler.try_pin_owned().unwrap();
    self.scheduler.publish_sync(pin, &self.metrics)
  }
}

fn block(byte: u8) -> &'static [u8] {
  Box::leak(vec![byte; BLOCK].into_boxed_slice())
}

fn written(task: &IOTask) -> (u64, Vec<*const u8>) {
  match task.get_type() {
    TaskType::Pwrite { offset, buf } => (offset, vec![buf.as_ptr()]),
    TaskType::Pwritev { offset, bufs } => {
      (offset, bufs.iter().map(|b| b.as_ptr()).collect())
    }
    _ => panic!("expected a write task"),
  }
}

/**
 * A request that is never fulfilled would block the wait forever, so the wait
 * is bounded to report a failure instead of hanging.
 */
fn result(done: Oneshot<Result<()>>) -> Result<()> {
  let (tx, rx) = mpsc::channel();
  thread::spawn(move || {
    let _ = tx.send(done.wait().unwrap());
  });
  rx.recv_timeout(TIMEOUT).expect("request was not fulfilled")
}

#[test]
fn test_scheduler_merges_adjacent_writes() {
  let f = Fixture::new(None);
  let first = f.write(0, block(0));
  let held = f.backend.take();

  let (x, y, z) = (block(1), block(2), block(3));
  let waiting = [
    f.write(2 * BLOCK as u64, x),
    f.write(BLOCK as u64, y),
    f.write(5 * BLOCK as u64, z),
  ];
  assert!(f.backend.is_idle());

  held.fulfill(Ok(BLOCK));
  let merged = f.backend.take();
  assert_eq!(
    written(&merged),
    (BLOCK as u64, vec![y.as_ptr(), x.as_ptr()])
  );
  let separate = f.backend.take();
  assert_eq!(written(&separate), (5 * BLOCK as u64, vec![z.as_ptr()]));
  assert!(f.backend.is_idle());

  merged.fulfill(Ok(2 * BLOCK));
  separate.fulfill(Ok(BLOCK));
  assert!(result(first).is_ok());
  waiting.into_iter().for_each(|w| assert!(result(w).is_ok()));
}

#[test]
fn test_scheduler_deduplicates_same_block() {
  let f = Fixture::new(None);
  let first = f.write(0, block(0));
  let held = f.backend.take();

  let (older, newer) = (block(1), block(2));
  let waiting = [f.write(BLOCK as u64, older), f.write(BLOCK as u64, newer)];

  held.fulfill(Ok(BLOCK));
  let task = f.backend.take();
  assert_eq!(written(&task), (BLOCK as u64, vec![newer.as_ptr()]));
  assert!(f.backend.is_idle());

  task.fulfill(Ok(BLOCK));
  assert!(result(first).is_ok());
  waiting.into_iter().for_each(|w| assert!(result(w).is_ok()));
}

#[test]
fn test_scheduler_allocates_before_first_write() {
  let f = Fixture::new(Some(0));
  let first = f.write(0, block(0));

  let allocation = f.backend.take();
  assert!(matches!(
    allocation.get_type(),
    TaskType::Fallocate {
      offset: 0,
      len: EXTENT
    }
  ));
  assert!(f.backend.is_idle());

  allocation.fulfill(Ok(0));
  let write = f.backend.take();
  assert_eq!(written(&write).0, 0);
  write.fulfill(Ok(BLOCK));
  assert!(result(first).is_ok());

  let inside = f.write(BLOCK as u64, block(1));
  let write = f.backend.take();
  assert_eq!(written(&write).0, BLOCK as u64);
  write.fulfill(Ok(BLOCK));
  assert!(result(inside).is_ok());

  let _beyond = f.write(EXTENT, block(2));
  assert!(matches!(
    f.backend.take().get_type(),
    TaskType::Fallocate {
      offset: EXTENT,
      len: EXTENT
    }
  ));
}

#[test]
fn test_scheduler_fallocate_failure() {
  let f = Fixture::new(Some(0));
  let first = f.write(0, block(0));

  f.backend
    .take()
    .fulfill(Err(Error::from(ErrorKind::StorageFull)));
  assert!(f.backend.is_idle());
  assert_eq!(result(first).unwrap_err().kind(), ErrorKind::StorageFull);

  let _retry = f.write(0, block(1));
  assert!(matches!(
    f.backend.take().get_type(),
    TaskType::Fallocate {
      offset: 0,
      len: EXTENT
    }
  ));
}

#[test]
fn test_scheduler_retries_short_write() {
  let f = Fixture::new(None);
  let data = block(0);
  let done = f.write(0, data);

  f.backend.take().fulfill(Ok(1));
  let retried = f.backend.take();
  assert_eq!(written(&retried), (0, vec![data.as_ptr()]));
  retried.fulfill(Ok(BLOCK));

  assert!(result(done).is_ok());
  assert!(f.backend.is_idle());
}

#[test]
fn test_scheduler_gives_up_short_write() {
  let f = Fixture::new(None);
  let done = f.write(0, block(0));

  for _ in 0..IO_RETRY {
    f.backend.take().fulfill(Ok(1));
  }

  assert!(f.backend.is_idle());
  assert_eq!(result(done).unwrap_err().kind(), ErrorKind::WriteZero);
}

#[test]
fn test_scheduler_merges_syncs() {
  let f = Fixture::new(None);
  let first = f.sync();
  let held = f.backend.take();
  assert!(matches!(held.get_type(), TaskType::Fdatasync));

  let waiting = [f.sync(), f.sync()];
  assert!(f.backend.is_idle());

  held.fulfill(Ok(0));
  let merged = f.backend.take();
  assert!(matches!(merged.get_type(), TaskType::Fdatasync));
  assert!(f.backend.is_idle());

  merged.fulfill(Err(Error::from(ErrorKind::Other)));
  assert!(result(first).is_ok());
  waiting
    .into_iter()
    .for_each(|w| assert_eq!(result(w).unwrap_err().kind(), ErrorKind::Other));
}

#[test]
fn test_scheduler_close_waits_for_published_requests() {
  let f = Fixture::new(None);
  let first = f.write(0, block(0));
  let held = f.backend.take();
  let queued = f.write(BLOCK as u64, block(1));

  let (tx, rx) = mpsc::channel();
  {
    let scheduler = f.scheduler.clone();
    thread::spawn(move || {
      scheduler.close();
      let _ = tx.send(());
    });
  }
  assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());

  held.fulfill(Ok(BLOCK));
  let next = f.backend.take();
  assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());

  next.fulfill(Ok(1));
  let retried = f.backend.take();
  assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());

  retried.fulfill(Ok(BLOCK));
  rx.recv_timeout(TIMEOUT).expect("close did not finish");

  assert!(result(first).is_ok());
  assert!(result(queued).is_ok());
  assert!(f.scheduler.try_pin().is_none());
  assert!(f.backend.is_idle());
}
