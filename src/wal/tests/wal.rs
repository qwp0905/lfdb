use std::{
  collections::HashMap,
  fs::{Metadata, ReadDir},
  io::{Error as IoError, ErrorKind, Result as IoResult},
  mem::take,
  path::Path,
  sync::{Condvar, Mutex, OnceLock},
  thread::scope,
};

use tempfile::TempDir;

use super::*;
use crate::{
  background::ThreadBuilder, DefaultDiskBackend, DiskBackend, IOBackend, IOTask, TaskType,
};

const TABLE: TableId = 1;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Op {
  Open,
  Write,
  Fdatasync,
  /**
   * Kept apart from `Fdatasync` so a sync on a file that never received a write,
   * such as the directory, never takes a fault armed for a commit.
   */
  UnwrittenFdatasync,
  Fsync,
  Fallocate,
}
impl Op {
  fn of(task: &IOTask, written: bool) -> Self {
    match task.context().get_type() {
      TaskType::Pwrite { .. } | TaskType::Pwritev { .. } => Self::Write,
      TaskType::Fdatasync if written => Self::Fdatasync,
      TaskType::Fdatasync => Self::UnwrittenFdatasync,
      TaskType::Fsync => Self::Fsync,
      TaskType::Fallocate { .. } => Self::Fallocate,
      TaskType::Open { .. } => Self::Open,
    }
  }
}

#[derive(Default)]
struct HoldState {
  opened: usize,
  written: usize,
  target: Option<usize>,
  failure: Option<ErrorKind>,
  held: Vec<IOTask>,
  fdatasynced: Vec<usize>,
}

#[derive(Clone, Default)]
struct FaultController {
  armed: Arc<Mutex<HashMap<Op, ErrorKind>>>,
  hold: Arc<(Mutex<HoldState>, Condvar)>,
}
impl FaultController {
  fn fail_next(&self, op: Op, kind: ErrorKind) {
    self.armed.lock().unwrap().insert(op, kind);
  }
  fn take(&self, op: Op) -> Option<ErrorKind> {
    self.armed.lock().unwrap().remove(&op)
  }

  fn count_open(&self) {
    let (lock, cvar) = &*self.hold;
    lock.lock().unwrap().opened += 1;
    cvar.notify_all();
  }
  fn opened(&self) -> usize {
    self.hold.0.lock().unwrap().opened
  }

  fn number(&self) -> usize {
    let (lock, cvar) = &*self.hold;
    let mut state = lock.lock().unwrap();
    let index = state.written;
    state.written += 1;
    cvar.notify_all();
    index
  }

  /**
   * Writes to the `index`-th written file stay in flight until `release`, so a
   * test can act while the WAL has not yet observed their outcome.
   */
  fn hold_writes(&self, index: usize) {
    self.hold.0.lock().unwrap().target = Some(index);
  }

  fn release(&self, kind: ErrorKind) {
    let held = {
      let mut state = self.hold.0.lock().unwrap();
      state.failure = Some(kind);
      take(&mut state.held)
    };
    // Fulfilled outside the lock because completion callbacks resubmit writes.
    for task in held {
      task.fulfill(Err(IoError::from(kind)));
    }
  }

  fn wait_until<F: Fn(&HoldState) -> bool>(&self, condition: F) {
    let (lock, cvar) = &*self.hold;
    let (state, waited) = cvar
      .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(10), |state| {
        !condition(state)
      })
      .unwrap();
    drop(state);
    assert!(!waited.timed_out(), "fault condition was never reached");
  }

  fn intercept(&self, index: Option<usize>, task: IOTask) -> Option<IOTask> {
    let Some(index) = index else {
      return Some(task);
    };
    let (lock, cvar) = &*self.hold;
    let mut state = lock.lock().unwrap();
    match task.context().get_type() {
      TaskType::Fdatasync => {
        state.fdatasynced.push(index);
        cvar.notify_all();
        return Some(task);
      }
      TaskType::Pwrite { .. } | TaskType::Pwritev { .. }
        if state.target == Some(index) => {}
      _ => return Some(task),
    }
    let Some(kind) = state.failure else {
      state.held.push(task);
      return None;
    };
    drop(state);
    task.fulfill(Err(IoError::from(kind)));
    None
  }
}

struct FaultIO {
  /**
   * Numbered by first write rather than by open, because opens complete out of
   * order while first writes follow the order the WAL starts using segments.
   */
  index: OnceLock<usize>,
  inner: Box<dyn IOBackend>,
  controller: FaultController,
}
impl IOBackend for FaultIO {
  fn submit(&self, task: IOTask) {
    if matches!(
      task.context().get_type(),
      TaskType::Pwrite { .. } | TaskType::Pwritev { .. }
    ) {
      self.index.get_or_init(|| self.controller.number());
    }
    let index = self.index.get().copied();
    let Some(task) = self.controller.intercept(index, task) else {
      return;
    };
    let Some(kind) = self.controller.take(Op::of(&task, index.is_some())) else {
      return self.inner.submit(task);
    };
    task.fulfill(Err(IoError::from(kind)));
  }
  fn pread(&self, buf: &mut [u8], offset: u64) -> IoResult<usize> {
    self.inner.pread(buf, offset)
  }
  fn metadata(&self) -> IoResult<Metadata> {
    self.inner.metadata()
  }
  fn try_flock(&self) -> IoResult<bool> {
    self.inner.try_flock()
  }
  fn unlock(&self) -> IoResult<()> {
    self.inner.unlock()
  }
}

struct FaultBackend {
  inner: DefaultDiskBackend,
  controller: FaultController,
}
impl DiskBackend for FaultBackend {
  fn submit_open(&self, task: IOTask) {
    self.controller.count_open();
    let Some(kind) = self.controller.take(Op::Open) else {
      return self.inner.submit_open(task);
    };
    task.fulfill(Err(IoError::from(kind)));
  }
  unsafe fn adopt(&self, raw: usize) -> Box<dyn IOBackend> {
    Box::new(FaultIO {
      index: OnceLock::new(),
      inner: unsafe { self.inner.adopt(raw) },
      controller: self.controller.clone(),
    })
  }
  fn read_dir(&self, path: &Path) -> IoResult<ReadDir> {
    self.inner.read_dir(path)
  }
  fn remove_file(&self, path: &Path) -> IoResult<()> {
    self.inner.remove_file(path)
  }
  fn exists(&self, path: &Path) -> IoResult<bool> {
    self.inner.exists(path)
  }
  fn rename(&self, from: &Path, to: &Path) -> IoResult<()> {
    self.inner.rename(from, to)
  }
  fn ensure_dir(&self, path: &Path) -> IoResult<()> {
    self.inner.ensure_dir(path)
  }
}

struct Fixture {
  _dir: TempDir,
  controller: FaultController,
  io_pool: Arc<IOPool>,
  event_bus: Arc<EventBus>,
  metrics: Arc<MetricsRegistry>,
  max_file_size: usize,
}
impl Fixture {
  fn new() -> Self {
    Self::with_segment_size(1 << 20)
  }

  fn with_segment_size(max_file_size: usize) -> Self {
    let dir = TempDir::new_in(".").unwrap();
    let controller = FaultController::default();
    let metrics = Arc::new(MetricsRegistry::new());
    let backend = FaultBackend {
      inner: DefaultDiskBackend::new().unwrap(),
      controller: controller.clone(),
    };
    let io_pool =
      Arc::new(IOPool::with_backend(backend, dir.path(), metrics.clone()).unwrap());
    Self {
      _dir: dir,
      controller,
      io_pool,
      event_bus: Arc::new(EventBus::new()),
      metrics,
      max_file_size,
    }
  }

  const fn config(&self) -> WALConfig {
    WALConfig {
      max_file_size: self.max_file_size,
      max_buffer_size: WALConfig::MIN_BUFFER_SIZE,
      segment_expiration: Duration::from_secs(60),
      segment_min_count: 1,
    }
  }

  fn open(&self) -> WriteAheadLog {
    WriteAheadLog::init(
      self.config(),
      self.event_bus.clone(),
      self.io_pool.clone(),
      self.metrics.clone(),
    )
    .unwrap()
  }

  fn replay(&self, wal: WriteAheadLog) -> ReplayResult {
    wal.close();
    drop(wal);

    let init_thread = ThreadBuilder::new().name("replay").count(1).pool();
    let (wal, result) = WriteAheadLog::replay(
      self.config(),
      self.event_bus.clone(),
      self.io_pool.clone(),
      self.metrics.clone(),
      &init_thread,
      WALFormatVersion::CURRENT,
    )
    .unwrap();
    wal.close();
    result
  }
}

fn insert(wal: &WriteAheadLog, tx_id: TxId) -> Result {
  insert_at(wal, tx_id, 0)
}

fn insert_at(wal: &WriteAheadLog, tx_id: TxId, ptr: Pointer) -> Result {
  wal
    .append_insert(tx_id, TABLE, ptr, tx_id, b"value")
    .map(drop)
}

fn commit(wal: &WriteAheadLog, tx_id: TxId) -> Result {
  wal.commit_and_flush(tx_id).map(drop)
}

#[test]
fn commit_returns_wal_failed_when_fdatasync_fails() {
  let fixture = Fixture::new();
  let wal = fixture.open();

  insert(&wal, 1).unwrap();
  commit(&wal, 1).unwrap();

  insert(&wal, 2).unwrap();
  fixture
    .controller
    .fail_next(Op::Fdatasync, ErrorKind::StorageFull);
  assert!(matches!(
    commit(&wal, 2),
    Err(Error::WALFailed(ErrorKind::StorageFull))
  ));

  assert!(!wal.is_available());
  assert!(matches!(
    insert(&wal, 3),
    Err(Error::WALFailed(ErrorKind::StorageFull))
  ));
  assert!(matches!(
    commit(&wal, 3),
    Err(Error::WALFailed(ErrorKind::StorageFull))
  ));

  let result = fixture.replay(wal);
  assert!(result.closed.contains(&1));
}

#[test]
fn commit_returns_wal_failed_when_block_write_fails() {
  let fixture = Fixture::new();
  let wal = fixture.open();

  insert(&wal, 1).unwrap();
  commit(&wal, 1).unwrap();

  // Armed after the insert because a non-flushing append still submits its block
  // write, which would otherwise take the fault meant for the commit.
  insert(&wal, 2).unwrap();
  fixture
    .controller
    .fail_next(Op::Write, ErrorKind::StorageFull);
  assert!(matches!(
    commit(&wal, 2),
    Err(Error::WALFailed(ErrorKind::StorageFull))
  ));

  let result = fixture.replay(wal);
  assert!(result.closed.contains(&1));
  assert!(!result.closed.contains(&2));
}

/**
 * Rotation publishes the next segment before the last write of the current one
 * completes, so a commit can become durable there while that write still fails.
 * Replay must then treat the commit as never happened rather than keep it without
 * the records lost in the rotated segment.
 */
#[test]
fn replay_keeps_tx_atomic_when_rotated_segment_write_fails() {
  const DURABLE: Pointer = 1;
  const LOST: Pointer = 2;

  let fixture = Fixture::with_segment_size(WAL_BLOCK_SIZE * 2);
  let wal = fixture.open();

  insert_at(&wal, 2, DURABLE).unwrap();
  commit(&wal, 1).unwrap();

  fixture.controller.hold_writes(0);
  insert_at(&wal, 2, LOST).unwrap();
  let opened = fixture.controller.opened();

  let committed = scope(|s| {
    let filler = s.spawn(|| while insert(&wal, 3).is_ok() {});

    // Rotation submits the next creation only after reserving the tail of the
    // first segment, so a commit issued from here on lands in the second.
    fixture.controller.wait_until(|state| state.opened > opened);
    let commit = s.spawn(|| commit(&wal, 2));

    fixture
      .controller
      .wait_until(|state| state.fdatasynced.contains(&1));
    fixture.controller.release(ErrorKind::StorageFull);

    filler.join().unwrap();
    commit.join().unwrap()
  });
  assert!(matches!(committed, Err(Error::WALFailed(_))));

  let result = fixture.replay(wal);
  let has = |ptr| {
    result
      .redo
      .iter()
      .any(|image| image.table_id == TABLE && image.pointer == ptr)
  };
  assert!(has(DURABLE));
  assert!(
    !result.closed.contains(&2) || has(LOST),
    "tx 2 replayed as committed without its lost record"
  );
}

/**
 * Segments are prepared one rotation ahead, so the failure surfaces within the
 * next rotations rather than at a fixed one.
 */
#[test]
fn rotation_returns_wal_failed_when_next_segment_cannot_be_prepared() {
  const MAX_INSERTS: usize = 100_000;

  for op in [Op::Open, Op::Fallocate, Op::Fsync, Op::UnwrittenFdatasync] {
    let fixture = Fixture::with_segment_size(WAL_BLOCK_SIZE * 2);
    let wal = fixture.open();

    insert(&wal, 1).unwrap();
    commit(&wal, 1).unwrap();

    fixture.controller.fail_next(op, ErrorKind::StorageFull);
    let failed = (0..MAX_INSERTS)
      .map(|_| insert(&wal, 2))
      .find(Result::is_err);
    assert!(matches!(
      failed,
      Some(Err(Error::WALFailed(ErrorKind::StorageFull)))
    ));
    assert!(!wal.is_available());

    let result = fixture.replay(wal);
    assert!(result.closed.contains(&1));
  }
}
