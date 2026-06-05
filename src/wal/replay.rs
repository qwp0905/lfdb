use std::{
  collections::{BTreeMap, BTreeSet},
  path::PathBuf,
};

use super::{read_page, LogId, Operation, TxId, FILE_EXT, WAL_BLOCK_SIZE};
use crate::{
  disk::{IOHandle, IOPool, PagePool, Pointer},
  error::Result,
  table::TableId,
};

pub const RESERVED_TX: TxId = 0;

/**
 * Output of WAL replay on startup.
 *
 * All Insert/Multi records are replayed unconditionally (redo-only) because
 * structural operations like B-tree splits cannot be safely undone — a crash
 * mid-split would leave the tree inconsistent if partial writes were skipped.
 *
 * aborted holds the set of transaction IDs that must be treated as rolled back
 * for MVCC visibility: explicitly aborted transactions plus any transactions
 * that were open (started but never committed or aborted) at the time of crash.
 */
pub struct ReplayResult {
  pub last_log_id: LogId,
  pub last_tx_id: TxId,
  pub aborted: BTreeSet<TxId>,
  pub started: BTreeSet<TxId>,
  pub closed: BTreeSet<TxId>,
  pub redo: Vec<(TableId, Pointer, Vec<u8>)>,
  pub segments: Vec<IOHandle>,
  pub last_snapshot: Option<PathBuf>,
}
impl ReplayResult {
  const fn empty() -> Self {
    Self {
      last_log_id: 0,
      last_tx_id: RESERVED_TX + 1,
      aborted: BTreeSet::new(),
      started: BTreeSet::new(),
      closed: BTreeSet::new(),
      redo: Vec::new(),
      segments: Vec::new(),
      last_snapshot: None,
    }
  }
}

pub fn replay(
  page_pool: &PagePool<WAL_BLOCK_SIZE>,
  io_pool: &IOPool,
) -> Result<ReplayResult> {
  let mut files = Vec::new();
  for file in io_pool.read_dir()? {
    let filename = PathBuf::from(file.file_name());
    if filename.extension().is_none_or(|ext| ext != FILE_EXT) {
      continue;
    }

    files.push(filename)
  }

  if files.len() == 0 {
    return Ok(ReplayResult::empty());
  }

  let mut tx_id = RESERVED_TX;
  let mut log_id = 0;
  let mut redo = BTreeMap::<LogId, (TableId, Pointer, Vec<u8>)>::new();
  let mut aborted = BTreeMap::<LogId, TxId>::new();
  let mut started = BTreeMap::<LogId, TxId>::new();
  let mut closed = BTreeMap::<LogId, TxId>::new();
  let mut last_snapshot = None;

  let mut segments = Vec::new();

  let mut last_checkpoint = None as Option<LogId>;
  for path in files.into_iter() {
    let segment = io_pool.open_direct_io(path)?;
    let len = segment.len()?;
    let mut offset = 0;
    let mut records = vec![];

    while offset < len {
      let mut page = page_pool.acquire();
      segment.read(page.as_mut(), offset)?;
      offset += WAL_BLOCK_SIZE as u64;

      let (r, complete) = read_page(&page);
      records.extend(r.into_iter());
      if complete {
        break;
      }
    }

    for record in records {
      log_id = record.log_id.max(log_id);
      tx_id = tx_id.max(record.tx_id);

      if last_checkpoint.is_some_and(|c| c > record.log_id) {
        continue;
      }

      match record.operation {
        Operation::Insert(table_id, ptr, page) => {
          redo.insert(record.log_id, (table_id, ptr, page));
        }
        Operation::Start => {
          started.insert(record.log_id, record.tx_id);
        }
        Operation::Commit => {
          closed.insert(record.log_id, record.tx_id);
        }
        Operation::Abort => {
          aborted.insert(record.log_id, record.tx_id);
        }
        Operation::Checkpoint(last_log_id, current_version, path) => {
          tx_id = tx_id.max(current_version);

          redo = redo.split_off(&last_log_id);
          aborted = aborted.split_off(&last_log_id);
          started = started.split_off(&last_log_id);
          closed = closed.split_off(&last_log_id);

          last_checkpoint = Some(last_log_id);
          last_snapshot = Some(path);
        }
      };
    }

    segments.push(segment);
  }

  Ok(ReplayResult {
    last_log_id: log_id + 1,
    last_tx_id: tx_id + 1,
    aborted: aborted.into_values().collect(),
    started: started.into_values().collect(),
    closed: closed.into_values().collect(),
    redo: redo.into_values().collect::<Vec<_>>(),
    segments,
    last_snapshot,
  })
}
