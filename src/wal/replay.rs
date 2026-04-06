use std::{
  collections::{BTreeMap, BTreeSet, HashSet},
  fs::read_dir,
};

use super::{
  LogId, Operation, SegmentGeneration, TxId, WALSegment, FILE_SUFFIX, WAL_BLOCK_SIZE,
};
use crate::{
  disk::{PagePool, Pointer},
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
  pub redo: Vec<(TableId, Pointer, Vec<u8>)>,
  pub segments: Vec<WALSegment>,
  pub generation: SegmentGeneration,
  pub is_new: bool,
}
impl ReplayResult {
  fn empty() -> Self {
    Self {
      last_log_id: 0,
      last_tx_id: RESERVED_TX + 1,
      generation: 0,
      aborted: Default::default(),
      redo: Default::default(),
      segments: Default::default(),
      is_new: true,
    }
  }
}

pub fn replay(
  base_dir: &str,
  flush_count: usize,
  page_pool: &PagePool<WAL_BLOCK_SIZE>,
) -> Result<ReplayResult> {
  let mut files = Vec::new();
  let mut generation = 0;
  for file in read_dir(base_dir).map_err(Error::IO)? {
    let file = file.map_err(Error::IO)?;
    if !file.file_name().to_string_lossy().ends_with(FILE_SUFFIX) {
      continue;
    }

    let current = WALSegment::parse_generation(&file.file_name().to_string_lossy())?;
    generation = generation.max(current + 1);
    files.push(file.path())
  }

  if files.len() == 0 {
    return Ok(ReplayResult::empty());
  }

  let mut tx_id = RESERVED_TX;
  let mut log_id = 0;
  let mut redo = BTreeMap::<LogId, Vec<(TableId, Pointer, Vec<u8>)>>::new();
  let mut aborted = BTreeMap::<LogId, TxId>::new();
  let mut started = BTreeSet::<TxId>::new();
  let mut closed = HashSet::<TxId>::new();

  let mut segments = Vec::new();

  let mut last_checkpoint = None as Option<LogId>;
  let mut last_min_active = None as Option<TxId>;
  for path in files.into_iter() {
    let wal = WALSegment::open_exists(&path, flush_count)?;
    let len = wal.len()?;
    let mut records = vec![];

    for i in 0..len {
      let mut page = page_pool.acquire();
      wal.read(i, &mut page)?;

      let (r, complete) = page.as_ref().into();
      records.extend(r.into_iter());
      if complete {
        break;
      }
    }

    for record in records {
      log_id = record.log_id.max(log_id);
      tx_id = tx_id.max(record.tx_id);
      match record.operation {
        Operation::Insert(table_id, ptr, page) => {
          if last_checkpoint.map_or(false, |c| c >= record.log_id) {
            continue;
          }
          redo
            .entry(record.log_id)
            .or_default()
            .push((table_id, ptr, page));
        }
        Operation::Multi(table_id, ptr1, data1, ptr2, data2) => {
          if last_checkpoint.map_or(false, |c| c >= record.log_id) {
            continue;
          }
          let e = redo.entry(record.log_id).or_default();
          e.push((table_id, ptr1, data1));
          e.push((table_id, ptr2, data2));
        }
        Operation::Start => {
          // Transactions older than min_active are already captured in the checkpoint's
          // abort set — no need to track them again.
          if let Some(&id) = last_min_active.as_ref() {
            if id > record.tx_id {
              continue;
            }
          }
          started.insert(record.tx_id);
        }
        Operation::Commit => {
          closed.insert(record.tx_id);
        }
        Operation::Abort => {
          closed.insert(record.tx_id);
          if last_checkpoint.map_or(false, |c| c >= record.log_id) {
            continue;
          }
          aborted.insert(record.log_id, record.tx_id);
        }
        Operation::Checkpoint(last_log_id, min_active) => {
          // Discard redo/abort entries already covered by the checkpoint.
          last_checkpoint = Some(last_checkpoint.unwrap_or(0).max(last_log_id));
          redo = redo.split_off(&last_log_id);
          aborted = aborted.split_off(&last_log_id);

          // Discard started entries below min_active — they are already reflected
          // in the checkpoint's abort set and don't need to be tracked again.
          last_min_active = Some(last_min_active.unwrap_or(0).max(min_active));
          started = started.split_off(&min_active)
        }
      };
    }

    segments.push(wal);
  }

  Ok(ReplayResult {
    last_log_id: log_id + 1,
    last_tx_id: tx_id + 1,
    // Explicit aborts + transactions open at crash time (started but never committed or aborted).
    aborted: aborted
      .into_values()
      .chain(started.into_iter().filter(|c| !closed.contains(&c)))
      .collect(),
    redo: redo.into_values().flatten().collect::<Vec<_>>(),
    segments,
    generation,
    is_new: false,
  })
}
