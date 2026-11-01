use std::{
  collections::{BTreeMap, BTreeSet},
  path::PathBuf,
  sync::Arc,
};

use crossbeam::channel::{unbounded, Sender};

use super::{LogId, LogRecord, Operation, TxId, WALFormatVersion, FILE_EXT};
use crate::{
  background::ThreadPool,
  blob::BlobMetadata,
  disk::{IOPool, Pointer, ScanIOHandle},
  error::Result,
  table::TableId,
  utils::debug,
  Error,
};

pub const RESERVED_TX: TxId = 0;

/**
 * WAL replay order is defined by `LogId`, not by segment file names or directory
 * iteration order.
 *
 * Segment files are just containers for WAL blocks. Replay scans every `.log`
 * file it can find, decodes valid records, and orders redo work by the log ids
 * embedded in those records.
 *
 * Checkpoint records trim the replay window.
 *
 * A checkpoint means every log before `last_log_id` is already represented
 * elsewhere: page changes are durable in table files, and transaction visibility
 * state is available through the version snapshot. Those older logs may be
 * replayable, but they are no longer needed for startup recovery.
 *
 * Replay keeps only the logs from the checkpoint's `last_log_id` up to the first
 * missing log id. A failed write or a crash can leave a hole while later
 * segments persist, and the records past the hole may complete a transaction
 * whose earlier records were lost.
 *
 * A checkpoint is trusted only when every log from its `last_log_id` to its own
 * record is present, because one written past a hole carries a snapshot that may
 * treat such a transaction as committed.
 *
 * WAL replay is redo-only.
 *
 * Insert records are replayed regardless of whether their transaction later
 * committed. Commit/abort state is reconstructed only for MVCC visibility; it
 * does not decide whether a page record is redone.
 */
pub struct ReplayResult {
  pub last_log_id: LogId,
  pub last_tx_id: TxId,
  pub started: BTreeSet<TxId>,
  pub closed: BTreeSet<TxId>,
  pub redo: Vec<RedoImage>,
  pub segments: Vec<ScanIOHandle>,
  pub last_snapshot: Option<PathBuf>,
  pub blob_handles: Vec<BlobMetadata>,
}

pub struct RedoImage {
  pub table_id: TableId,
  pub pointer: Pointer,
  pub data: Vec<u8>,
}
impl RedoImage {
  const fn new(table_id: TableId, pointer: Pointer, data: Vec<u8>) -> Self {
    Self {
      table_id,
      pointer,
      data,
    }
  }
}

pub fn replay(
  io_pool: Arc<IOPool>,
  init_thread: &ThreadPool,
  _version: WALFormatVersion,
) -> Result<ReplayResult> {
  let mut files = Vec::new();
  for file in io_pool.read_dir()? {
    let filename = PathBuf::from(file.file_name());
    if filename.extension().is_none_or(|ext| ext != FILE_EXT) {
      continue;
    }

    files.push(filename)
  }

  debug!("trying to replay {} segments.", files.len());

  let (tx, rx) = unbounded();
  let forked = init_thread.fork(files.into_iter(), scan_segment(io_pool, tx));

  let mut state = ReplayState::new();
  while let Ok(record) = rx.recv() {
    state.apply(record)?;
  }

  let segments = forked.join().collect::<Result<Vec<_>>>()?;
  Ok(state.into_result(segments))
}

struct CheckpointState {
  start: LogId,
  snapshot: PathBuf,
  missing: usize,
}
impl CheckpointState {
  const fn new(start: LogId, snapshot: PathBuf, missing: usize) -> Self {
    Self {
      start,
      snapshot,
      missing,
    }
  }
  const fn is_complete(&self) -> bool {
    self.missing == 0
  }

  const fn covers(&self, id: LogId, log_id: LogId) -> bool {
    self.start <= log_id && log_id <= id
  }

  const fn fill(&mut self) -> bool {
    self.missing -= 1;
    self.is_complete()
  }

  const fn track(&mut self, id: LogId, log_id: LogId) -> bool {
    !self.is_complete() && self.covers(id, log_id) && self.fill()
  }
}

struct ReplayState {
  last_log_id: LogId,
  last_tx_id: TxId,
  redo: BTreeMap<LogId, RedoImage>,
  started: BTreeMap<LogId, TxId>,
  closed: BTreeMap<LogId, TxId>,
  blob_handles: BTreeMap<LogId, BlobMetadata>,
  checkpoints: BTreeMap<LogId, CheckpointState>,
}
impl ReplayState {
  const fn new() -> Self {
    Self {
      last_log_id: 0,
      last_tx_id: RESERVED_TX,
      redo: BTreeMap::new(),
      started: BTreeMap::new(),
      closed: BTreeMap::new(),
      blob_handles: BTreeMap::new(),
      checkpoints: BTreeMap::new(),
    }
  }

  fn base(&self) -> Option<(&LogId, &CheckpointState)> {
    self
      .checkpoints
      .iter()
      .rev()
      .find(|(_, state)| state.is_complete())
  }

  fn floor(&self) -> LogId {
    self.base().map(|(_, state)| state.start).unwrap_or(0)
  }

  fn apply(&mut self, record: LogRecord) -> Result {
    self.last_tx_id = self.last_tx_id.max(record.tx_id + 1);
    self.last_log_id = self.last_log_id.max(record.log_id + 1);
    if record.log_id < self.floor() || self.contains(&record.log_id) {
      return Ok(());
    }

    self.track(record.log_id);
    self.insert(record)
  }

  fn track(&mut self, log_id: LogId) {
    let mut settled = None;
    for (id, state) in self.checkpoints.iter_mut() {
      if state.track(*id, log_id) {
        settled = Some(*id);
      }
    }
    let Some(id) = settled else {
      return;
    };
    self.settle(id);
  }

  fn settle(&mut self, id: LogId) {
    if self.base().is_some_and(|(base, _)| *base > id) {
      return;
    }
    let start = self.checkpoints[&id].start;
    self.checkpoints = self.checkpoints.split_off(&start);
    self.redo = self.redo.split_off(&start);
    self.started = self.started.split_off(&start);
    self.closed = self.closed.split_off(&start);
    self.blob_handles = self.blob_handles.split_off(&start);
  }

  fn insert(&mut self, record: LogRecord) -> Result {
    match record.operation {
      Operation::Insert {
        table_id,
        pointer,
        data,
        current_version,
        encoding,
        original_len,
      } => {
        let Ok(decoded) = encoding.decompress(&data, original_len as usize) else {
          return Err(Error::CompressionCrashed(encoding));
        };
        let image = RedoImage::new(table_id, pointer, decoded);
        self.redo.insert(record.log_id, image);
        self.started.insert(record.log_id, record.tx_id);
        self.last_tx_id = self.last_tx_id.max(current_version);
      }
      Operation::Commit => {
        self.closed.insert(record.log_id, record.tx_id);
      }
      Operation::Checkpoint {
        last_log_id,
        current_version,
        snapshot,
      } => {
        self.last_tx_id = self.last_tx_id.max(current_version);
        let missing = (last_log_id..record.log_id)
          .filter(|id| !self.contains(id))
          .count();
        let state = CheckpointState::new(last_log_id, snapshot, missing);
        self.checkpoints.insert(record.log_id, state);
        if missing == 0 {
          self.settle(record.log_id);
        }
      }
      Operation::BlobCreated(metadata) => {
        self.blob_handles.insert(record.log_id, metadata);
      }
    };
    Ok(())
  }

  fn contains(&self, id: &LogId) -> bool {
    self.redo.contains_key(id)
      || self.closed.contains_key(id)
      || self.blob_handles.contains_key(id)
      || self.checkpoints.contains_key(id)
  }

  fn first_missing(&self, start: LogId) -> LogId {
    (start..).find(|id| !self.contains(id)).unwrap()
  }

  fn first_log_id(&self) -> LogId {
    let first = [
      self.redo.keys().next(),
      self.closed.keys().next(),
      self.blob_handles.keys().next(),
      self.checkpoints.keys().next(),
    ];
    first.into_iter().flatten().min().copied().unwrap_or(0)
  }

  fn into_result(self, segments: Vec<ScanIOHandle>) -> ReplayResult {
    let (start, last_snapshot) = match self.base() {
      Some((_, state)) => (state.start, Some(state.snapshot.clone())),
      None => (self.first_log_id(), None),
    };
    let end = self.first_missing(start);

    ReplayResult {
      last_log_id: self.last_log_id,
      last_tx_id: self.last_tx_id,
      started: within(self.started, start, end).collect(),
      closed: within(self.closed, start, end).collect(),
      redo: within(self.redo, start, end).collect(),
      segments,
      last_snapshot,
      blob_handles: within(self.blob_handles, start, end).collect(),
    }
  }
}

fn within<V>(
  mut logs: BTreeMap<LogId, V>,
  start: LogId,
  end: LogId,
) -> impl Iterator<Item = V> {
  let mut kept = logs.split_off(&start);
  kept.split_off(&end);
  kept.into_values()
}

const fn scan_segment(
  io_pool: Arc<IOPool>,
  channel: Sender<LogRecord>,
) -> impl Fn(PathBuf) -> Result<ScanIOHandle> {
  move |path| {
    let mut segment = io_pool.open_scan_io(path)?;
    let len = segment.len()?;

    while segment.get_offset() + (LogRecord::LEN_BYTES as u64) < len {
      let byte_len = u16::from_le_bytes(segment.read_array()?) as usize;
      if segment.get_offset() + (byte_len as u64) > len {
        break;
      }

      let buf = segment.read_to_vec(byte_len)?;
      let Some(record) = LogRecord::read_from(&buf) else {
        break;
      };
      channel.send(record).unwrap();
    }
    Ok(segment)
  }
}
