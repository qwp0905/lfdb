use std::{io, result};

use thiserror::Error;

use crate::wal::RecordEncoding;

#[derive(Debug, Error)]
pub enum Error {
  #[error("table {0} not found")]
  TableNotFound(String),

  #[error("invalid format: {0}")]
  InvalidFormat(&'static str),

  #[error("invalid block type expected {0} received {1}")]
  DeserializeError(u8, u8),

  #[error("io error: {0:?}")]
  IO(io::Error),

  #[error("end of file")]
  EOF,

  #[error("transaction already closed")]
  TransactionClosed,

  #[error("engine unavailable")]
  EngineUnavailable,

  #[error("flush failed")]
  FlushFailed,

  #[error("write conflict detected")]
  WriteConflict,

  #[error("thread conflict detected")]
  ThreadConflict,

  #[error("exceeded maximum table name length. maximum {0}, received {1}")]
  TableNameExceeded(usize, usize),

  #[error("table name is empty.")]
  TableNameEmpty,

  #[error("character '{0}' is not allowed in table names (only alphanumeric, '-', and '_' are permitted)")]
  NotAllowedChar(char),

  #[error("exceeded maximum key length. maximum {0}, received {1}")]
  KeyExceeded(usize, usize),

  #[error("exceeded maximum value length. maximum {0}, received {1}")]
  ValueExceeded(usize, usize),

  #[error("wal unavailable to write. please drop engine and restart.")]
  WALUnavailable,

  #[error("wal failed since {0}. please drop engine and restart.")]
  WALFailed(io::ErrorKind),

  #[error("failed to open base dir.")]
  DirOpenFailed,

  #[error("invalidate config: {0}.")]
  InvalidConfig(&'static str),

  #[error("compression crashed at algorithm: {0:?}")]
  CompressionCrashed(RecordEncoding),
}

pub type Result<T = ()> = result::Result<T, Error>;
