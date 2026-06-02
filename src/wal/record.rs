use std::{ffi::OsStr, path::PathBuf, ptr::copy_nonoverlapping, slice::from_raw_parts};

use super::{LogId, TxId, WAL_BLOCK_SIZE};
use crate::{
  disk::{Page, Pointer, POINTER_BYTES},
  table::{TableId, TABLE_ID_BYTES},
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

  unsafe fn write_at(&self, ptr: *mut u8) {
    let mut offset = 4;
    copy_nonoverlapping(
      self.log_id.to_le_bytes().as_ptr(),
      ptr.add(offset),
      LOG_ID_BYTES,
    );
    offset += LOG_ID_BYTES;

    copy_nonoverlapping(
      self.tx_id.to_le_bytes().as_ptr(),
      ptr.add(offset),
      TX_ID_BYTES,
    );
    offset += TX_ID_BYTES;

    *ptr.add(offset) = self.operation.type_byte();
    offset += 1;
    match &self.operation {
      Operation::Insert(table_id, page_ptr, data) => {
        copy_nonoverlapping(
          table_id.to_le_bytes().as_ptr(),
          ptr.add(offset),
          TABLE_ID_BYTES,
        );
        offset += TABLE_ID_BYTES;
        copy_nonoverlapping(
          page_ptr.to_le_bytes().as_ptr(),
          ptr.add(offset),
          POINTER_BYTES,
        );
        offset += POINTER_BYTES;
        let data_len = data.len();
        copy_nonoverlapping(data.as_ptr(), ptr.add(offset), data_len);
        offset += data_len;
      }
      Operation::Checkpoint(log_id, current_version, path) => {
        copy_nonoverlapping(log_id.to_le_bytes().as_ptr(), ptr.add(offset), LOG_ID_BYTES);
        offset += LOG_ID_BYTES;

        copy_nonoverlapping(
          current_version.to_le_bytes().as_ptr(),
          ptr.add(offset),
          TX_ID_BYTES,
        );
        offset += TX_ID_BYTES;

        copy_nonoverlapping(
          path.as_os_str().as_encoded_bytes().as_ptr(),
          ptr.add(offset),
          path.as_os_str().len(),
        );
        offset += path.as_os_str().len();
      }
      Operation::Start => {}
      Operation::Commit => {}
      Operation::Abort => {}
    };

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(unsafe { from_raw_parts(ptr.add(4), offset - 4) });
    let checksum = hasher.finalize().to_le_bytes();
    unsafe { copy_nonoverlapping(checksum.as_ptr(), ptr, 4) };
  }

  fn byte_len(&self) -> u16 {
    self.operation.byte_len() + 4 + LOG_ID_BYTES as u16 + TX_ID_BYTES as u16
  }
  pub fn to_bytes_with_len(&self) -> Vec<u8> {
    let len = self.byte_len();
    let mut vec = vec![0; (len + 2) as usize];
    let ptr = vec.as_mut_ptr();
    unsafe { copy_nonoverlapping((len as u16).to_le_bytes().as_ptr(), ptr, 2) };
    unsafe { self.write_at(ptr.add(2)) };
    vec
  }

  pub fn to_bytes(&self) -> Vec<u8> {
    let len = self.byte_len();
    let mut vec = vec![0; len as usize];
    unsafe { self.write_at(vec.as_mut_ptr()) };
    vec
  }
}

impl TryFrom<&[u8]> for LogRecord {
  type Error = Error;

  fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
    let len = value.len();
    if len < 21 {
      return Err(Error::InvalidFormat("log record too short."));
    }
    let ptr = value.as_ptr();

    let checksum = u32::from_le_bytes(unsafe { (ptr as *const [u8; 4]).read() });
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(unsafe { from_raw_parts(ptr.add(4), len - 4) });
    if hasher.finalize() != checksum {
      return Err(Error::InvalidFormat("checksum not matched."));
    }

    let mut offset = 5 + LOG_ID_BYTES + TX_ID_BYTES;
    let log_id =
      LogId::from_le_bytes(unsafe { (ptr.add(4) as *const [u8; LOG_ID_BYTES]).read() });
    let tx_id = TxId::from_le_bytes(unsafe {
      (ptr.add(4 + LOG_ID_BYTES) as *const [u8; TX_ID_BYTES]).read()
    });
    let operation = match unsafe { *ptr.add(offset - 1) } {
      1 => unsafe {
        let table_id =
          TableId::from_le_bytes((ptr.add(offset) as *const [u8; TABLE_ID_BYTES]).read());
        offset += TABLE_ID_BYTES;
        let page_ptr =
          Pointer::from_le_bytes((ptr.add(offset) as *const [u8; POINTER_BYTES]).read());
        offset += POINTER_BYTES;

        let mut data = vec![0; len - offset];
        copy_nonoverlapping(ptr.add(offset), data.as_mut_ptr(), data.len());
        Operation::Insert(table_id, page_ptr, data)
      },
      2 => {
        if len != offset {
          return Err(Error::InvalidFormat("invalid len for start log."));
        }
        Operation::Start
      }
      3 => {
        if len != offset {
          return Err(Error::InvalidFormat("invalid len for commit log."));
        }
        Operation::Commit
      }
      4 => {
        if len != offset {
          return Err(Error::InvalidFormat("invalid len for abort log."));
        };
        Operation::Abort
      }
      5 => unsafe {
        if len < offset + TX_ID_BYTES + LOG_ID_BYTES {
          return Err(Error::InvalidFormat("invalid len for checkpoint log."));
        }
        let log_id =
          LogId::from_le_bytes((ptr.add(offset) as *const [u8; LOG_ID_BYTES]).read());
        offset += LOG_ID_BYTES;

        let current_version =
          TxId::from_le_bytes((ptr.add(offset) as *const [u8; TX_ID_BYTES]).read());
        offset += TX_ID_BYTES;

        let path = OsStr::from_encoded_bytes_unchecked(from_raw_parts(
          ptr.add(offset),
          len - offset,
        ));
        Operation::Checkpoint(log_id, current_version, path.into())
      },
      _ => return Err(Error::InvalidFormat("invalid type log record.")),
    };
    Ok(LogRecord::new(log_id, tx_id, operation))
  }
}

impl From<&Page<WAL_BLOCK_SIZE>> for (Vec<LogRecord>, bool) {
  fn from(value: &Page<WAL_BLOCK_SIZE>) -> Self {
    let mut data = vec![];
    let mut scanner = value.scanner();
    let len = match scanner.read_u16() {
      Ok(l) => l,
      Err(_) => return (data, true), // ignore error cause of partial write
    };

    for _ in 0..len {
      let size = match scanner.read_u16() {
        Ok(s) => s,
        Err(_) => return (data, true), // ignore error cause of partial write
      };
      match scanner.read_n(size as usize).and_then(|p| p.try_into()) {
        Ok(record) => data.push(record),
        Err(_) => return (data, true), // ignore error cause of partial write
      }
    }
    (data, false)
  }
}

pub fn read_page(value: &Page<WAL_BLOCK_SIZE>) -> (Vec<LogRecord>, bool) {
  let mut data = vec![];
  let mut scanner = value.scanner();
  let len = match scanner.read_u16() {
    Ok(l) => l,
    Err(_) => return (data, true), // ignore error cause of partial write
  };

  for _ in 0..len {
    let size = match scanner.read_u16() {
      Ok(s) => s,
      Err(_) => return (data, true), // ignore error cause of partial write
    };
    match scanner.read_n(size as usize).and_then(|p| p.try_into()) {
      Ok(record) => data.push(record),
      Err(_) => return (data, true), // ignore error cause of partial write
    }
  }
  (data, false)
}

#[cfg(test)]
#[path = "tests/record.rs"]
mod tests;
