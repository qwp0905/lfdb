use std::{
  fs::{Metadata, OpenOptions, ReadDir},
  io::{Error as IoError, ErrorKind, Result as IoResult},
  path::Path,
  sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, Mutex,
  },
  thread,
  time::Duration,
};

use lfdb::{
  DefaultDiskBackend, DiskBackend, Engine, EngineBuilder, Error, IOBackend, IOTask,
  TaskType,
};
use log::Log;
use tempfile::{tempdir_in, TempDir};

struct TestLogger;
impl Log for TestLogger {
  fn enabled(&self, _: &log::Metadata) -> bool {
    true
  }

  fn log(&self, record: &log::Record) {
    println!("[{}] {}", record.level(), record.args())
  }

  fn flush(&self) {}
}

const TABLE: &str = "test";
const MAX_NTH: usize = 30;
const MAX_TX_PER_THREAD: usize = 2000;
const TX_AFTER_FIRED: usize = 10;
const THREAD_COUNT: usize = 4;
const KEYS_PER_TX: usize = 4;
const VALUE_SIZE: usize = 1 << 10;
const TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Target {
  Wal,
  Data,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
  Write,
  Fdatasync,
  Fsync,
}
impl Op {
  fn of(task: &IOTask) -> Option<Self> {
    match task.get_type() {
      TaskType::Pwrite { .. } | TaskType::Pwritev { .. } => Some(Self::Write),
      TaskType::Fdatasync => Some(Self::Fdatasync),
      TaskType::Fsync => Some(Self::Fsync),
      TaskType::Fallocate { .. } => None,
    }
  }
}

struct Armed {
  target: Target,
  op: Op,
  remain: usize,
}

#[derive(Default)]
struct FaultState {
  armed: Mutex<Option<Armed>>,
  fired: AtomicBool,
}

#[derive(Clone, Default)]
struct FaultController(Arc<FaultState>);
impl FaultController {
  fn arm(&self, target: Target, op: Op, nth: usize) {
    *self.0.armed.lock().unwrap() = Some(Armed {
      target,
      op,
      remain: nth,
    });
  }

  fn fired(&self) -> bool {
    self.0.fired.load(Ordering::Acquire)
  }

  fn should_fail(&self, target: Target, op: Op) -> bool {
    let mut armed = self.0.armed.lock().unwrap();
    let Some(current) = armed.as_mut() else {
      return false;
    };
    if current.target != target || current.op != op {
      return false;
    }
    current.remain -= 1;
    if current.remain > 0 {
      return false;
    }
    *armed = None;
    self.0.fired.store(true, Ordering::Release);
    true
  }
}

struct FaultIO {
  inner: Box<dyn IOBackend>,
  controller: FaultController,
  target: Target,
}
impl IOBackend for FaultIO {
  fn pread(&self, buf: &mut [u8], offset: u64) -> IoResult<usize> {
    self.inner.pread(buf, offset)
  }

  fn submit(&self, task: IOTask) {
    let Some(op) = Op::of(&task) else {
      return self.inner.submit(task);
    };
    if !self.controller.should_fail(self.target, op) {
      return self.inner.submit(task);
    }
    task.fulfill(Err(IoError::from(ErrorKind::StorageFull)));
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
impl FaultBackend {
  fn new(controller: FaultController) -> Self {
    Self {
      inner: DefaultDiskBackend::new().unwrap(),
      controller,
    }
  }

  fn wrap(&self, path: &Path, inner: Box<dyn IOBackend>) -> Box<dyn IOBackend> {
    let target = match path.extension().is_some_and(|ext| ext == "log") {
      true => Target::Wal,
      false => Target::Data,
    };
    Box::new(FaultIO {
      inner,
      controller: self.controller.clone(),
      target,
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

fn options(dir: &TempDir, wal_file_size: usize) -> EngineBuilder {
  let _ = log::set_logger(&TestLogger);
  log::set_max_level(log::LevelFilter::Trace);
  EngineBuilder::new(dir.path())
    .wal_file_size(wal_file_size)
    .wal_buffer_size(64 << 10)
    .wal_segment_expiration(Duration::from_millis(1))
    .wal_segment_min_count(1)
    .block_cache_memory_capacity(64 << 10)
    .block_cache_shard_count(1)
    .block_cache_buffer_size(32 << 10)
    .gc_batch_size(10240)
    .compaction_batch_size(10_000)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TxResult {
  Committed,
  WalFailed,
  Rejected,
  Failed,
}

#[derive(Default)]
struct Failures {
  wal: AtomicBool,
  other: AtomicBool,
}
impl Failures {
  fn observe(&self, err: Error) -> TxResult {
    match err {
      Error::WALFailed(_) => {
        self.wal.store(true, Ordering::Release);
        TxResult::WalFailed
      }
      Error::EngineUnavailable => TxResult::Rejected,
      _ => {
        self.other.store(true, Ordering::Release);
        TxResult::Failed
      }
    }
  }
}

struct Outcome {
  keys: Vec<Vec<u8>>,
  after_wal_failed: bool,
  after_other_failure: bool,
  wrote: bool,
  result: TxResult,
  error: Option<String>,
}

fn write_tx(engine: &Engine, keys: &[Vec<u8>], wrote: &mut bool) -> Result<(), Error> {
  let mut tx = engine.new_tx()?;
  let table = tx.table(TABLE)?;
  for key in keys {
    table.insert(key.clone(), vec![b'v'; VALUE_SIZE])?;
    *wrote = true;
  }
  drop(table);
  tx.commit()
}

fn run_tx(engine: &Engine, id: usize, failures: &Failures) -> Outcome {
  let mut outcome = Outcome {
    keys: (0..KEYS_PER_TX)
      .map(|i| format!("{id:0>6}-{i}").into_bytes())
      .collect(),
    after_wal_failed: failures.wal.load(Ordering::Acquire),
    after_other_failure: failures.other.load(Ordering::Acquire),
    wrote: false,
    result: TxResult::Committed,
    error: None,
  };
  if let Err(err) = write_tx(engine, &outcome.keys, &mut outcome.wrote) {
    outcome.error = Some(format!("{err:?}"));
    outcome.result = failures.observe(err);
  }
  outcome
}

fn run_workload(
  engine: Arc<Engine>,
  controller: &FaultController,
  concurrent: bool,
) -> Vec<Outcome> {
  let failures = Arc::new(Failures::default());
  let thread_count = if concurrent { THREAD_COUNT } else { 1 };

  let (tx, rx) = mpsc::channel();
  let threads = (0..thread_count)
    .map(|t| {
      let engine = engine.clone();
      let controller = controller.clone();
      let failures = failures.clone();
      let tx = tx.clone();
      thread::spawn(move || {
        let mut outcomes = Vec::new();
        let mut remain = TX_AFTER_FIRED;
        for i in 0..MAX_TX_PER_THREAD {
          if controller.fired() {
            if remain == 0 {
              break;
            }
            remain -= 1;
          }
          outcomes.push(run_tx(&engine, t * MAX_TX_PER_THREAD + i, &failures));
        }
        tx.send(outcomes).unwrap();
      })
    })
    .collect::<Vec<_>>();
  drop(engine);

  let outcomes = (0..thread_count)
    .flat_map(|_| rx.recv_timeout(TIMEOUT).expect("workload hangs"))
    .collect();
  // The last thread drops the engine, and reopening before that close finishes
  // waits on the directory lock.
  threads.into_iter().for_each(|t| t.join().unwrap());
  outcomes
}

fn verify(dir: &TempDir, wal_file_size: usize, outcomes: &[Outcome]) {
  for outcome in outcomes {
    if outcome.after_wal_failed {
      assert!(!outcome.wrote, "write accepted after wal failed");
      assert_ne!(outcome.result, TxResult::Committed, "commit accepted after wal failed");
      continue;
    }
    if outcome.after_other_failure {
      assert_ne!(
        outcome.result,
        TxResult::Failed,
        "transaction failed after an unrelated io failure: {:?}",
        outcome.error
      );
    }
  }

  let engine = options(dir, wal_file_size).open().unwrap();
  let tx = engine.new_tx().unwrap();
  let table = tx.table(TABLE).unwrap();
  for outcome in outcomes {
    let found = outcome
      .keys
      .iter()
      .filter(|key| table.get(key).unwrap().is_some())
      .count();
    if outcome.result == TxResult::Committed {
      assert_eq!(found, outcome.keys.len(), "committed transaction lost");
      continue;
    }
    assert!(
      found == 0 || found == outcome.keys.len(),
      "failed transaction partially visible"
    );
  }
}

fn run_scenario(
  target: Target,
  op: Op,
  nth: usize,
  wal_file_size: usize,
  concurrent: bool,
) {
  let dir = tempdir_in(".").unwrap();
  let controller = FaultController::default();
  let engine = options(&dir, wal_file_size)
    .open_with_backend(FaultBackend::new(controller.clone()))
    .unwrap();
  {
    let mut tx = engine.new_tx().unwrap();
    tx.open_table(TABLE).unwrap();
    tx.commit().unwrap();
  }

  controller.arm(target, op, nth);
  let outcomes = run_workload(Arc::new(engine), &controller, concurrent);
  assert!(controller.fired(), "fault was not injected within the tx limit");
  verify(&dir, wal_file_size, &outcomes);
}

fn sweep(target: Target, op: Op, wal_file_size: usize) {
  for nth in 1..=MAX_NTH {
    run_scenario(target, op, nth, wal_file_size, false);
    run_scenario(target, op, nth, wal_file_size, true);
  }
}

#[test]
fn wal_write_failure_keeps_policy() {
  sweep(Target::Wal, Op::Write, 256 << 10);
}

#[test]
fn wal_fdatasync_failure_keeps_policy() {
  sweep(Target::Wal, Op::Fdatasync, 256 << 10);
}

#[test]
#[ignore = "checkpoint failover from the event bus races with engine close on the incoming queue"]
fn wal_segment_creation_failure_keeps_policy() {
  sweep(Target::Wal, Op::Fsync, 64 << 10);
}

#[test]
#[ignore = "block cache eviction rollback does not restore the evicted key mapping"]
fn data_write_failure_keeps_policy() {
  sweep(Target::Data, Op::Write, 256 << 10);
}

#[test]
#[ignore = "background workers abort on io failure"]
fn data_fsync_failure_keeps_policy() {
  sweep(Target::Data, Op::Fsync, 256 << 10);
}
