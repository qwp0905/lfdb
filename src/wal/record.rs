use std::{ffi::OsStr, path::PathBuf};

use super::{RecordEncoding, TxId, WriteSeq};
use crate::{
  blob::BlobMetadata,
  disk::{Pointer, POINTER_BYTES},
  table::{TableId, TABLE_ID_BYTES},
  utils::{OffsetReader, OffsetWriter},
  wal::{TX_ID_BYTES, WRITE_SEQ_BYTES},
};

#[derive(Debug)]
pub enum Operation {
  Insert {
    table_id: TableId,
    pointer: Pointer,         // disk pointer of the page
    current_version: TxId,    // current version at the time the wal record was written
    data: Vec<u8>,            // data
    original_len: u16,        // original len before compression
    encoding: RecordEncoding, // encoding of compression
  },
  Commit,
  /**
   * Records the last stable write_seq and the minimum active transaction version.
   * During recovery, min_active bounds the abort set — transactions that started
   * before min_active can be discarded, preventing the abort set from growing
   * unboundedly.
   */
  Checkpoint {
    durable_write_seq: WriteSeq, // durable write seq
    current_version: TxId,       // current version
    snapshot: PathBuf,           // mvcc snapshot path
  },
  BlobCreated(BlobMetadata), // created blob metadata
}
impl Operation {
  const fn type_byte(&self) -> u8 {
    match self {
      Self::Insert { .. } => 1,
      Self::Commit => 2,
      Self::Checkpoint { .. } => 3,
      Self::BlobCreated(_) => 4,
    }
  }

  fn byte_len(&self) -> usize {
    1 + match self {
      Self::Insert { data, .. } => {
        POINTER_BYTES + TABLE_ID_BYTES + TX_ID_BYTES + data.len() + 1 + 2
      }
      Self::Checkpoint { snapshot, .. } => {
        TX_ID_BYTES + WRITE_SEQ_BYTES + snapshot.as_os_str().len()
      }
      Self::BlobCreated(metadata) => metadata.byte_len(),
      _ => 0,
    }
  }
}

#[derive(Debug)]
pub struct LogRecord {
  pub write_seq: WriteSeq,
  pub tx_id: TxId,
  pub operation: Operation,
}
impl LogRecord {
  #[inline]
  const fn new(write_seq: WriteSeq, tx_id: TxId, operation: Operation) -> Self {
    Self {
      tx_id,
      operation,
      write_seq,
    }
  }

  pub const LEN_BYTES: usize = 2;
  pub const MIN_BYTES: usize = Self::LEN_BYTES + 4 + (TX_ID_BYTES << 1) + 1;

  pub fn read_from(buf: &[u8]) -> Option<Self> {
    let mut reader = OffsetReader::new(buf);
    let checksum = reader.read_u32()?;

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&buf[4..]);
    if hasher.finalize() != checksum {
      return None;
    }

    let write_seq = reader.read_u64()?;
    let tx_id = reader.read_u64()?;

    let operation = match reader.read_byte()? {
      1 => {
        let table_id = reader.read_u32()?;
        let pointer = reader.read_u64()?;
        let current_version = reader.read_u64()?;
        let original_len = reader.read_u16()?;
        let encoding = RecordEncoding::from_byte(reader.read_byte()?)?;
        let data = reader.read_all();
        Operation::Insert {
          table_id,
          pointer,
          current_version,
          data: data.to_vec(),
          original_len,
          encoding,
        }
      }
      2 => (reader.is_eof()).then_some(Operation::Commit)?,
      3 => {
        let write_seq = reader.read_u64()?;
        let current_version = reader.read_u64()?;
        let path = unsafe { OsStr::from_encoded_bytes_unchecked(reader.read_all()) };
        Operation::Checkpoint {
          durable_write_seq: write_seq,
          current_version,
          snapshot: path.into(),
        }
      }
      4 => Operation::BlobCreated(BlobMetadata::read_from(&mut reader)?),
      _ => return None,
    };
    Some(LogRecord::new(write_seq, tx_id, operation))
  }

  fn write_at(&self, buf: &mut [u8]) {
    let mut writer = OffsetWriter::new(&mut buf[4..]);
    writer.write_u64(self.write_seq);
    writer.write_u64(self.tx_id);

    writer.write_u8(self.operation.type_byte());

    match &self.operation {
      Operation::Insert {
        table_id,
        pointer,
        data,
        current_version,
        original_len,
        encoding,
      } => {
        writer.write_u32(*table_id);
        writer.write_u64(*pointer);
        writer.write_u64(*current_version);
        writer.write_u16(*original_len);
        writer.write_u8(*encoding as u8);
        writer.write(data);
      }
      Operation::Checkpoint {
        durable_write_seq,
        current_version,
        snapshot,
      } => {
        writer.write_u64(*durable_write_seq);
        writer.write_u64(*current_version);
        writer.write(snapshot.as_os_str().as_encoded_bytes());
      }
      Operation::Commit => {}
      Operation::BlobCreated(metadata) => {
        metadata.write_at(&mut writer);
      }
    }
    debug_assert_eq!(writer.written_bytes(), buf.len() - 4);

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&buf[4..]);
    let checksum = hasher.finalize().to_le_bytes();
    buf[..4].copy_from_slice(&checksum);
  }
}

/**
 * WAL record with its final size known but without an assigned `LogId`.
 *
 * The record body and encoded length are prepared first. The `LogId` is attached
 * later by `init`, when the caller is ready to produce the final WAL bytes.
 */
pub struct LogRecordUninit {
  buf: Vec<u8>,
  tx_id: TxId,
  operation: Operation,
}
impl LogRecordUninit {
  fn new(tx_id: TxId, operation: Operation) -> Self {
    let len = operation.byte_len() + 4 + WRITE_SEQ_BYTES + TX_ID_BYTES;
    let mut buf = vec![0; len + LogRecord::LEN_BYTES];
    buf[..LogRecord::LEN_BYTES].copy_from_slice(&(len as u16).to_le_bytes());
    Self {
      buf,
      tx_id,
      operation,
    }
  }
  pub const fn len(&self) -> usize {
    self.buf.len()
  }
  pub fn init(mut self, write_seq: WriteSeq) -> Vec<u8> {
    let record = LogRecord::new(write_seq, self.tx_id, self.operation);
    record.write_at(&mut self.buf[LogRecord::LEN_BYTES..]);
    self.buf
  }

  pub fn new_insert(
    tx_id: TxId,
    table_id: TableId,
    pointer: Pointer,
    current_version: TxId,
    encoding: RecordEncoding,
    data: &[u8],
  ) -> Self {
    let original_len = data.len() as u16;
    let data = encoding.compress(data);
    Self::new(
      tx_id,
      Operation::Insert {
        table_id,
        pointer,
        data,
        current_version,
        encoding,
        original_len,
      },
    )
  }

  pub fn new_commit(tx_id: TxId) -> Self {
    Self::new(tx_id, Operation::Commit)
  }

  /**
   * Build a checkpoint control record.
   *
   * The outer `tx_id` field has no meaning for checkpoint records; replay uses the
   * checkpoint payload (`durable_write_seq`, `current_version`, and `snapshot_path`)
   * instead.
   */
  pub fn new_checkpoint(
    durable_write_seq: WriteSeq,
    current_version: TxId,
    snapshot: PathBuf,
  ) -> Self {
    Self::new(
      0,
      Operation::Checkpoint {
        durable_write_seq,
        current_version,
        snapshot,
      },
    )
  }
  pub fn new_blob_created(metadata: BlobMetadata) -> Self {
    Self::new(0, Operation::BlobCreated(metadata))
  }
}

#[cfg(all(test, not(loom)))]
#[path = "tests/record.rs"]
mod tests;
