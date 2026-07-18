use std::{
  collections::{BTreeMap, BTreeSet},
  path::PathBuf,
};

use super::{LogId, LogRecord, Operation, TxId, FILE_EXT};
use crate::{
  disk::{IOPool, Pointer, ScanIOHandle},
  error::Result,
  table::TableId,
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
  pub redo: Vec<(TableId, Pointer, Vec<u8>)>,
  pub segments: Vec<ScanIOHandle>,
  pub last_snapshot: Option<PathBuf>,
}
impl ReplayResult {
  const fn empty() -> Self {
    Self {
      last_log_id: 0,
      last_tx_id: RESERVED_TX + 1,
      started: BTreeSet::new(),
      closed: BTreeSet::new(),
      redo: Vec::new(),
      segments: Vec::new(),
      last_snapshot: None,
    }
  }
}

pub fn replay(io_pool: &IOPool) -> Result<ReplayResult> {
  let mut files = Vec::new();
  for file in io_pool.read_dir()? {
    let filename = PathBuf::from(file.file_name());
    if filename.extension().is_none_or(|ext| ext != FILE_EXT) {
      continue;
    }

    files.push(filename)
  }

  if files.is_empty() {
    return Ok(ReplayResult::empty());
  }

  let mut tx_id = RESERVED_TX;
  let mut log_id = 0;
  let mut redo = BTreeMap::<LogId, (TableId, Pointer, Vec<u8>)>::new();
  let mut started = BTreeMap::<LogId, TxId>::new();
  let mut closed = BTreeMap::<LogId, TxId>::new();
  let mut last_snapshot = None;

  let mut segments = Vec::new();

  let mut last_checkpoint: Option<LogId> = None;
  for path in files {
    let mut segment = io_pool.open_scan_io(path)?;
    let len = segment.len();

    while segment.get_offset() + (LogRecord::LEN_BYTES as u64) < len {
      let mut buf = [0; LogRecord::LEN_BYTES];
      segment.read(&mut buf)?;
      let byte_len = u16::from_le_bytes(buf) as usize;
      if segment.get_offset() + (byte_len as u64) > len {
        break;
      }

      let buf = segment.read_to_vec(byte_len)?;
      let Some(record) = LogRecord::read_from(&buf) else {
        break;
      };

      tx_id = tx_id.max(record.current_version + 1);

      if last_checkpoint.is_some_and(|c| c > record.log_id) {
        continue;
      }
      log_id = log_id.max(record.log_id + 1);

      match record.operation {
        Operation::Insert {
          table_id,
          pointer,
          data,
          encoding,
          original_len,
        } => {
          let Ok(decoded) = encoding.decompress(&data, original_len as usize) else {
            return Err(Error::CompressionCrashed(encoding));
          };
          redo.insert(record.log_id, (table_id, pointer, decoded));
          started.insert(record.log_id, record.current_version);
        }
        Operation::Commit => {
          closed.insert(record.log_id, record.current_version);
        }
        Operation::Checkpoint {
          last_log_id,
          snapshot,
        } => {
          redo = redo.split_off(&last_log_id);
          started = started.split_off(&last_log_id);
          closed = closed.split_off(&last_log_id);

          last_checkpoint = Some(last_log_id);
          last_snapshot = Some(snapshot);
        }
      };
    }

    segments.push(segment);
  }

  Ok(ReplayResult {
    last_log_id: log_id,
    last_tx_id: tx_id,
    started: started.into_values().collect(),
    closed: closed.into_values().collect(),
    redo: redo.into_values().collect::<Vec<_>>(),
    segments,
    last_snapshot,
  })
}
