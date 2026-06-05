use std::{ffi::OsStr, path::PathBuf};

use super::{LogId, TxId, WAL_BLOCK_SIZE};
use crate::{
  disk::{Page, Pointer, POINTER_BYTES},
  table::{TableId, TABLE_ID_BYTES},
  utils::{OffsetReader, OffsetWriter},
  wal::{LOG_ID_BYTES, TX_ID_BYTES},
  Error,
};

#[derive(Debug)]
pub enum Operation {
  Insert(
    TableId,
    Pointer, // disk pointer of the page
    Vec<u8>, // data
  ),
  Start,
  Commit,
  Abort,
  /**
   * Records the last stable log_id and the minimum active transaction version.
   * During recovery, min_active bounds the abort set — transactions that started
   * before min_active can be discarded, preventing the abort set from growing
   * unboundedly.
   */
  Checkpoint(
    LogId,   // last log id
    TxId,    // current version
    PathBuf, // mvcc snapshot path
  ),
}
impl Operation {
  const fn type_byte(&self) -> u8 {
    match self {
      Self::Insert(..) => 1,
      Self::Start => 2,
      Self::Commit => 3,
      Self::Abort => 4,
      Self::Checkpoint(..) => 5,
    }
  }

  fn byte_len(&self) -> u16 {
    1 + match self {
      Self::Insert(_, _, data) => {
        POINTER_BYTES as u16 + TABLE_ID_BYTES as u16 + data.len() as u16
      }
      Self::Checkpoint(_, _, path) => {
        TX_ID_BYTES as u16 + LOG_ID_BYTES as u16 + path.as_os_str().len() as u16
      }
      _ => 0,
    }
  }
}

#[derive(Debug)]
pub struct LogRecord {
  pub log_id: LogId,
  pub tx_id: TxId,
  pub operation: Operation,
}
impl LogRecord {
  #[inline]
  const fn new(log_id: LogId, tx_id: TxId, operation: Operation) -> Self {
    Self {
      tx_id,
      operation,
      log_id,
    }
  }
  pub const fn new_insert(
    log_id: LogId,
    tx_id: TxId,
    table_id: TableId,
    page_pointer: Pointer,
    data: Vec<u8>,
  ) -> Self {
    Self::new(
      log_id,
      tx_id,
      Operation::Insert(table_id, page_pointer, data),
    )
  }

  pub const fn new_start(log_id: LogId, tx_id: TxId) -> Self {
    Self::new(log_id, tx_id, Operation::Start)
  }

  pub const fn new_commit(log_id: LogId, tx_id: TxId) -> Self {
    Self::new(log_id, tx_id, Operation::Commit)
  }

  pub const fn new_abort(log_id: LogId, tx_id: TxId) -> Self {
    Self::new(log_id, tx_id, Operation::Abort)
  }

  pub const fn new_checkpoint(
    log_id: LogId,
    last_log_id: LogId,
    current_version: TxId,
    snapshot_path: PathBuf,
  ) -> Self {
    Self::new(
      log_id,
      0,
      Operation::Checkpoint(last_log_id, current_version, snapshot_path),
    )
  }

  fn read_from(buf: &[u8]) -> Option<Self> {
    let mut reader = OffsetReader::new(&buf);
    let checksum = reader.read_u32()?;

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&buf[4..]);
    if hasher.finalize() != checksum {
      return None;
    }

    let log_id = reader.read_u64()?;
    let tx_id = reader.read_u64()?;

    let operation = match reader.read_byte()? {
      1 => {
        let table_id = reader.read_u32()?;
        let page_ptr = reader.read_u64()?;
        let data = reader.read_all()?;
        Operation::Insert(table_id, page_ptr, data.to_vec())
      }
      2 => (reader.is_eof()).then(|| Operation::Start)?,
      3 => (reader.is_eof()).then(|| Operation::Commit)?,
      4 => (reader.is_eof()).then(|| Operation::Abort)?,
      5 => {
        let log_id = reader.read_u64()?;
        let current_version = reader.read_u64()?;
        let path = unsafe { OsStr::from_encoded_bytes_unchecked(reader.read_all()?) };
        Operation::Checkpoint(log_id, current_version, path.into())
      }
      _ => return None,
    };
    Some(LogRecord::new(log_id, tx_id, operation))
  }

  fn write_at(&self, buf: &mut [u8]) {
    let mut writer = OffsetWriter::new(&mut buf[4..]);
    writer.write_u64(self.log_id);
    writer.write_u64(self.tx_id);

    writer.write_u8(self.operation.type_byte());

    match &self.operation {
      Operation::Insert(table_id, page_ptr, data) => {
        writer.write_u32(*table_id);
        writer.write_u64(*page_ptr);
        writer.write(data);
      }
      Operation::Checkpoint(log_id, current_version, path) => {
        writer.write_u64(*log_id);
        writer.write_u64(*current_version);
        writer.write(path.as_os_str().as_encoded_bytes());
      }
      Operation::Start => {}
      Operation::Commit => {}
      Operation::Abort => {}
    }
    debug_assert_eq!(writer.written_bytes(), buf.len() - 4);

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&buf[4..]);
    let checksum = hasher.finalize().to_le_bytes();
    buf[..4].copy_from_slice(&checksum);
  }

  fn byte_len(&self) -> u16 {
    self.operation.byte_len() + 4 + LOG_ID_BYTES as u16 + TX_ID_BYTES as u16
  }
  pub fn to_bytes_with_len(&self) -> Vec<u8> {
    let len = self.byte_len();
    let mut vec = vec![0; (len + 2) as usize];
    vec[..2].copy_from_slice(&(len as u16).to_le_bytes());
    self.write_at(&mut vec[2..]);
    vec
  }
}

impl TryFrom<&[u8]> for LogRecord {
  type Error = Error;

  fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
    LogRecord::read_from(value)
      .map(Ok)
      .unwrap_or_else(|| Err(Error::InvalidFormat("log record parse failed.")))
  }
}

pub fn read_page(value: &Page<WAL_BLOCK_SIZE>) -> (Vec<LogRecord>, bool) {
  let mut scanner = value.scanner();
  let len = match scanner.read_u16() {
    Ok(l) => l as usize,
    Err(_) => return (vec![], true), // ignore error cause of partial write
  };

  let mut data = Vec::with_capacity(len);
  for _ in 0..len {
    let size = match scanner.read_u16() {
      Ok(s) => s as usize,
      Err(_) => return (data, true), // ignore error cause of partial write
    };
    match scanner.read_n(size).and_then(|p| p.try_into()) {
      Ok(record) => data.push(record),
      Err(_) => return (data, true), // ignore error cause of partial write
    }
  }
  (data, false)
}

#[cfg(test)]
#[path = "tests/record.rs"]
mod tests;
