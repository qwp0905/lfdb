use std::{ptr::copy_nonoverlapping, slice::from_raw_parts};

use super::{LogId, TxId};
use crate::{
  disk::{Page, Pointer},
  Error,
};

// Sized to hold at least 2 base pages (base page = 8KB) with room for headers.
pub const WAL_BLOCK_SIZE: usize = 16 << 10; // 16kb

#[derive(Debug)]
pub enum Operation {
  Insert(
    Pointer, // disk pointer of the page
    Vec<u8>, // data
  ),
  /**
   * Records two page writes as a single atomic operation.
   * Used during B-tree node merge to prevent duplicate keys from becoming
   * visible between the two page updates.
   * Limited to two pages because acquiring three or more page locks
   * simultaneously is avoided by design to prevent deadlocks.
   */
  Multi(
    Pointer, // disk pointer of the first page
    Vec<u8>, // data of the first page
    Pointer, // disk pointer of the second page
    Vec<u8>, // data of the second page
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
    LogId, // last log id
    TxId,  // min active version
  ),
}
impl Operation {
  fn type_byte(&self) -> u8 {
    match self {
      Operation::Insert(..) => 1,
      Operation::Start => 2,
      Operation::Commit => 3,
      Operation::Abort => 4,
      Operation::Checkpoint(..) => 5,
      Operation::Multi(..) => 6,
    }
  }

  const CHECKPOINT_LEN: u16 = 1 + 8 + 8;
  const OTHER_LEN: u16 = 1;
  fn byte_len(&self) -> u16 {
    match self {
      Operation::Insert(_, data) => 1 + 8 + data.len() as u16,
      Operation::Multi(_, d1, _, d2) => 1 + 16 + 2 + d1.len() as u16 + d2.len() as u16,
      Operation::Checkpoint(_, _) => Self::CHECKPOINT_LEN,
      _ => Self::OTHER_LEN,
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
  fn new(log_id: LogId, tx_id: TxId, operation: Operation) -> Self {
    LogRecord {
      tx_id,
      operation,
      log_id,
    }
  }
  pub fn new_insert(
    log_id: LogId,
    tx_id: TxId,
    page_pointer: Pointer,
    data: Vec<u8>,
  ) -> Self {
    LogRecord::new(log_id, tx_id, Operation::Insert(page_pointer, data))
  }

  pub fn new_start(log_id: LogId, tx_id: TxId) -> Self {
    LogRecord::new(log_id, tx_id, Operation::Start)
  }

  pub fn new_commit(log_id: LogId, tx_id: TxId) -> Self {
    LogRecord::new(log_id, tx_id, Operation::Commit)
  }

  pub fn new_abort(log_id: LogId, tx_id: TxId) -> Self {
    LogRecord::new(log_id, tx_id, Operation::Abort)
  }

  pub fn new_checkpoint(log_id: LogId, last_log_id: LogId, min_active: TxId) -> Self {
    LogRecord::new(log_id, 0, Operation::Checkpoint(last_log_id, min_active))
  }
  pub fn new_multi(
    log_id: LogId,
    tx_id: TxId,
    ptr1: Pointer,
    data1: Vec<u8>,
    ptr2: Pointer,
    data2: Vec<u8>,
  ) -> Self {
    LogRecord::new(log_id, tx_id, Operation::Multi(ptr1, data1, ptr2, data2))
  }

  unsafe fn write_at(&self, ptr: *mut u8) {
    let mut offset = 4;
    copy_nonoverlapping(self.log_id.to_le_bytes().as_ptr(), ptr.add(offset), 8);
    offset += 8;

    copy_nonoverlapping(self.tx_id.to_le_bytes().as_ptr(), ptr.add(offset), 8);
    offset += 8;

    *ptr.add(offset) = self.operation.type_byte();
    offset += 1;
    match &self.operation {
      Operation::Insert(page_ptr, data) => {
        copy_nonoverlapping(page_ptr.to_le_bytes().as_ptr(), ptr.add(offset), 8);
        offset += 8;
        let data_len = data.len();
        copy_nonoverlapping(data.as_ptr(), ptr.add(offset), data_len);
        offset += data_len;
      }
      Operation::Checkpoint(log_id, min_active) => {
        copy_nonoverlapping(log_id.to_le_bytes().as_ptr(), ptr.add(offset), 8);
        offset += 8;
        copy_nonoverlapping(min_active.to_le_bytes().as_ptr(), ptr.add(offset), 8);
        offset += 8;
      }
      Operation::Start => {}
      Operation::Commit => {}
      Operation::Abort => {}
      Operation::Multi(ptr1, data1, ptr2, data2) => {
        copy_nonoverlapping(ptr1.to_le_bytes().as_ptr(), ptr.add(offset), 8);
        offset += 8;
        let data_len = data1.len();
        copy_nonoverlapping((data_len as u16).to_le_bytes().as_ptr(), ptr.add(offset), 2);
        offset += 2;

        copy_nonoverlapping(data1.as_ptr(), ptr.add(offset), data_len);
        offset += data_len;

        copy_nonoverlapping(ptr2.to_le_bytes().as_ptr(), ptr.add(offset), 8);
        offset += 8;
        let data_len = data2.len();
        copy_nonoverlapping(data2.as_ptr(), ptr.add(offset), data_len);
        offset += data_len;
      }
    };

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(unsafe { from_raw_parts(ptr.add(4), offset - 4) });
    let checksum = hasher.finalize().to_le_bytes();
    unsafe { copy_nonoverlapping(checksum.as_ptr(), ptr, 4) };
  }

  fn byte_len(&self) -> u16 {
    self.operation.byte_len() + 4 + 8 + 8
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

    let mut offset = 21;
    let log_id = LogId::from_le_bytes(unsafe { (ptr.add(4) as *const [u8; 8]).read() });
    let tx_id = TxId::from_le_bytes(unsafe { (ptr.add(12) as *const [u8; 8]).read() });
    let operation = match unsafe { *ptr.add(20) } {
      1 => unsafe {
        let index = Pointer::from_le_bytes((ptr.add(offset) as *const [u8; 8]).read());
        offset += 8;

        let mut data = vec![0; len - offset];
        copy_nonoverlapping(ptr.add(offset), data.as_mut_ptr(), data.len());
        Operation::Insert(index, data)
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
        if len != offset + 16 {
          return Err(Error::InvalidFormat("invalid len for checkpoint log."));
        }
        let log_id = LogId::from_le_bytes((ptr.add(offset) as *const [u8; 8]).read());
        offset += 8;
        let min_active = TxId::from_le_bytes((ptr.add(offset) as *const [u8; 8]).read());
        Operation::Checkpoint(log_id, min_active)
      },
      6 => unsafe {
        let ptr1 = Pointer::from_le_bytes((ptr.add(offset) as *const [u8; 8]).read());
        offset += 8;

        let data_len =
          u16::from_le_bytes((ptr.add(offset) as *const [u8; 2]).read()) as usize;
        offset += 2;

        let mut data1 = vec![0; data_len];
        copy_nonoverlapping(ptr.add(offset), data1.as_mut_ptr(), data_len);
        offset += data_len;

        let ptr2 = Pointer::from_le_bytes((ptr.add(offset) as *const [u8; 8]).read());
        offset += 8;

        let mut data2 = vec![0; len - offset];
        copy_nonoverlapping(ptr.add(offset), data2.as_mut_ptr(), data2.len());

        Operation::Multi(ptr1, data1, ptr2, data2)
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

#[cfg(test)]
#[path = "tests/record.rs"]
mod tests;
