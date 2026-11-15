use std::{io, result};

use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
  /**
   * No table with this name exists. Check the name, or create the table first.
   */
  #[error("table {0} not found")]
  TableNotFound(String),

  /**
   * Stored data did not decode as expected. Either storage was damaged or the
   * engine broke its own invariants; in both cases nothing read from it can be
   * trusted. Restore from a disk-level backup.
   */
  #[error("data corrupted: {0}")]
  Corrupted(&'static str),

  /**
   * A file operation failed. Check the storage device and file permissions;
   * transient failures may succeed when retried.
   */
  #[error("io error: {0:?}")]
  IO(io::Error),

  /**
   * The transaction was already committed or aborted. Start a new transaction.
   */
  #[error("transaction already closed")]
  TransactionClosed,

  /**
   * The engine is shutting down or has been closed. Open the engine again.
   */
  #[error("engine unavailable")]
  EngineUnavailable,

  /**
   * Another transaction changed the same key first. Abort and retry the
   * transaction.
   */
  #[error("write conflict detected")]
  WriteConflict,

  /**
   * The table name is longer than the maximum. Use a shorter name.
   */
  #[error("exceeded maximum table name length. maximum {0}, received {1}")]
  TableNameExceeded(usize, usize),

  /**
   * The table name is empty. Use a non-empty name.
   */
  #[error("table name is empty.")]
  TableNameEmpty,

  /**
   * The table name contains a character outside alphanumerics, '-' and '_'.
   * Remove or replace the character.
   */
  #[error("character '{0}' is not allowed in table names (only alphanumeric, '-', and '_' are permitted)")]
  NotAllowedChar(char),

  /**
   * The key is longer than the maximum. Use a shorter key.
   */
  #[error("exceeded maximum key length. maximum {0}, received {1}")]
  KeyExceeded(usize, usize),

  /**
   * The value is longer than the maximum. Use a smaller value.
   */
  #[error("exceeded maximum value length. maximum {0}, received {1}")]
  ValueExceeded(usize, usize),

  /**
   * The write-ahead log failed and no further write can be made durable. Drop the
   * engine and open it again.
   */
  #[error("wal failed since {0}. please drop engine and restart.")]
  WALFailed(io::ErrorKind),

  /**
   * The base directory stayed locked by another process. Make sure no other
   * engine instance uses the same directory.
   */
  #[error("failed to open base dir.")]
  DirOpenFailed,

  /**
   * A configuration value is out of range. Fix the configuration.
   */
  #[error("invalidate config: {0}.")]
  InvalidConfig(&'static str),

  /**
   * Stored data was written in a format version this build does not support.
   * Open it with a build that supports that version.
   */
  #[error("version unsupported.")]
  UnsupportedVersion,

  /**
   * Stored data was written with a different page size than this build uses.
   * Open it with a build of the same page size.
   */
  #[error("page size unsupported.")]
  UnsupportedPageSize,
}

pub type Result<T = ()> = result::Result<T, Error>;
