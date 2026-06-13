use std::{
  io,
  panic::{RefUnwindSafe, UnwindSafe},
  result,
};

use thiserror::Error;

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

  #[error("un allowed char {}", 0)]
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
}

pub type Result<T = ()> = result::Result<T, Error>;
unsafe impl Send for Error {}
unsafe impl Sync for Error {}
impl RefUnwindSafe for Error {}
impl UnwindSafe for Error {}
