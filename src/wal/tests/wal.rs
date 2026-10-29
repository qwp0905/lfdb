use std::{
  fs::{Metadata, OpenOptions, ReadDir},
  io::{Error as IoError, ErrorKind, Result as IoResult},
  path::Path,
  sync::Mutex,
};

use tempfile::TempDir;

use super::*;
use crate::{
  background::ThreadBuilder, DefaultDiskBackend, DiskBackend, IOBackend, IOTask, TaskType,
};

const TABLE: TableId = 1;

#[derive(Clone, Default)]
struct FaultController {
  write: Arc<Mutex<Option<ErrorKind>>>,
  fdatasync: Arc<Mutex<Option<ErrorKind>>>,
}
impl FaultController {
  fn fail_next_write(&self, kind: ErrorKind) {
    *self.write.lock().unwrap() = Some(kind);
  }
  fn fail_next_fdatasync(&self, kind: ErrorKind) {
    *self.fdatasync.lock().unwrap() = Some(kind);
  }
}

struct FaultIO {
  inner: Box<dyn IOBackend>,
  controller: FaultController,
}
impl IOBackend for FaultIO {
  fn submit(&self, task: IOTask) {
    let slot = match task.get_type() {
      TaskType::Pwrite { .. } | TaskType::Pwritev { .. } => &self.controller.write,
      TaskType::Fdatasync => &self.controller.fdatasync,
      TaskType::Fsync | TaskType::Fallocate { .. } => return self.inner.submit(task),
    };
    let Some(kind) = slot.lock().unwrap().take() else {
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

/**
 * Only segment files are wrapped, because the preloader syncs the directory in
 * the background and would otherwise consume a fault armed for a commit.
 */
struct FaultBackend {
  inner: DefaultDiskBackend,
  controller: FaultController,
}
impl FaultBackend {
  fn wrap(&self, path: &Path, inner: Box<dyn IOBackend>) -> Box<dyn IOBackend> {
    if path.extension().is_none_or(|ext| ext != "log") {
      return inner;
    }
    Box::new(FaultIO {
      inner,
      controller: self.controller.clone(),
    })
  }
}
impl DiskBackend for FaultBackend {
  fn open(&self, options: &mut OpenOptions, path: &Path) -> IoResult<Box<dyn IOBackend>> {
    self
      .inner
      .open(options, path)
      .map(|inner| self.wrap(path, inner))
  }
  fn open_direct_io(
    &self,
    options: &mut OpenOptions,
    path: &Path,
  ) -> IoResult<Box<dyn IOBackend>> {
    self
      .inner
      .open_direct_io(options, path)
      .map(|inner| self.wrap(path, inner))
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
}
impl Fixture {
  fn new() -> Self {
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
    }
  }

  fn config() -> WALConfig {
    WALConfig {
      max_file_size: 1 << 20,
      max_buffer_size: WALConfig::MIN_BUFFER_SIZE,
      segment_expiration: Duration::from_secs(60),
      segment_min_count: 1,
    }
  }

  fn open(&self) -> WriteAheadLog {
    WriteAheadLog::init(
      Self::config(),
      self.event_bus.clone(),
      self.io_pool.clone(),
      self.metrics.clone(),
    )
    .unwrap()
  }

  fn replay(&self, wal: WriteAheadLog) -> ReplayResult {
    wal.close();
    drop(wal);

    let init_thread = ThreadBuilder::new().name("replay").multi(1).pool();
    let (wal, result) = WriteAheadLog::replay(
      Self::config(),
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
  wal
    .append_insert(tx_id, TABLE, 0, tx_id, b"value")
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
    .fail_next_fdatasync(ErrorKind::StorageFull);
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
  fixture.controller.fail_next_write(ErrorKind::StorageFull);
  assert!(matches!(
    commit(&wal, 2),
    Err(Error::WALFailed(ErrorKind::StorageFull))
  ));

  let result = fixture.replay(wal);
  assert!(result.closed.contains(&1));
  assert!(!result.closed.contains(&2));
}
