use std::{
  collections::{BTreeMap, BTreeSet},
  fs::read_dir,
  path::{Path, PathBuf},
};

use super::{
  LogId, Operation, SegmentGeneration, TxId, WALSegment, FILE_EXT, WAL_BLOCK_SIZE,
};
use crate::{
  disk::{Page, PagePool, Pointer},
  error::{Error, Result},
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
  pub segments: Vec<WALSegment>,
  pub generation: SegmentGeneration,
  pub last_snapshot: Option<PathBuf>,
}
impl ReplayResult {
  const fn empty() -> Self {
    Self {
      last_log_id: 0,
      last_tx_id: RESERVED_TX + 1,
      generation: 0,
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
  base_dir: &Path,
  flush_count: usize,
  page_pool: &PagePool<WAL_BLOCK_SIZE>,
) -> Result<ReplayResult> {
  let mut files = Vec::new();
  let mut generation = 0;
  for file in read_dir(base_dir).map_err(Error::IO)? {
    let path = file.map_err(Error::IO)?.path();
    if path.extension().map_or(true, |ext| ext != FILE_EXT) {
      continue;
    }

    let current = WALSegment::parse_generation(&path)?;
    generation = generation.max(current + 1);
    files.push(path)
  }

  if files.len() == 0 {
    return Ok(ReplayResult::empty());
  }

  let mut tx_id = RESERVED_TX;
  let mut log_id = 0;
  let mut redo = BTreeMap::<LogId, Vec<(TableId, Pointer, Vec<u8>)>>::new();
  let mut aborted = BTreeMap::<LogId, TxId>::new();
  let mut started = BTreeMap::<LogId, TxId>::new();
  let mut closed = BTreeMap::<LogId, TxId>::new();
  let mut last_snapshot = None;

  let mut segments = Vec::new();

  let mut last_checkpoint = None as Option<LogId>;
  for path in files.into_iter() {
    let wal = WALSegment::open_exists(&path, flush_count)?;
    let len = wal.len()?;
    let mut records = vec![];

    for i in 0..len {
      let mut page = page_pool.acquire();
      wal.read(i, &mut page)?;

      let (r, complete) = (&page as &Page<_>).into();
      records.extend(r.into_iter());
      if complete {
        break;
      }
    }

    for record in records {
      log_id = record.log_id.max(log_id);
      tx_id = tx_id.max(record.tx_id);

      if last_checkpoint.map_or(false, |c| c > record.log_id) {
        continue;
      }

      match record.operation {
        Operation::Insert(table_id, ptr, page) => {
          redo
            .entry(record.log_id)
            .or_default()
            .push((table_id, ptr, page));
        }
        Operation::Multi(table_id, ptr1, data1, ptr2, data2) => {
          let e = redo.entry(record.log_id).or_default();
          e.push((table_id, ptr1, data1));
          e.push((table_id, ptr2, data2));
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

    segments.push(wal);
  }

  Ok(ReplayResult {
    last_log_id: log_id + 1,
    last_tx_id: tx_id + 1,
    aborted: aborted.into_values().collect(),
    started: started.into_values().collect(),
    closed: closed.into_values().collect(),
    redo: redo.into_values().flatten().collect::<Vec<_>>(),
    segments,
    generation,
    last_snapshot,
  })
}
