use std::{any::Any, error, io, result, sync::Arc};

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

  #[error("worker closed")]
  WorkerClosed,

  #[error("flush failed")]
  FlushFailed,

  #[error("write conflict detected")]
  WriteConflict,

  #[error("thread conflict detected")]
  ThreadConflict,

  #[error("channel disconnected")]
  ChannelDisconnected,

  #[error("exceeded maximum key length. maximum {0}, received {1}")]
  KeyExceeded(usize, usize),

  #[error("exceeded maximum value length. maximum {0}, received {1}")]
  ValueExceeded(usize, usize),

  #[error("thread panic: {0:?}")]
  Panic(Arc<dyn Any + Send>),

  #[error("unknown {0:?}")]
  Unknown(String),
}
impl Error {
  pub fn unknown<E>(err: E) -> Self
  where
    E: error::Error + Send + Sync + 'static,
  {
    Self::Unknown(err.to_string())
  }
  pub fn panic(err: Arc<dyn Any + Send>) -> Self {
    Self::Panic(err)
  }
}
impl Clone for Error {
  fn clone(&self) -> Self {
    match self {
      Self::TableNotFound(str) => Self::TableNotFound(str.clone()),
      Self::InvalidFormat(err) => Self::InvalidFormat(err),
      Self::DeserializeError(e, r) => Self::DeserializeError(*e, *r),
      Self::Unknown(err) => Self::Unknown(err.clone()),
      Self::IO(err) => Self::IO(io::Error::new(err.kind(), err.to_string())),
      Self::EOF => Self::EOF,
      Self::TransactionClosed => Self::TransactionClosed,
      Self::EngineUnavailable => Self::EngineUnavailable,
      Self::WorkerClosed => Self::WorkerClosed,
      Self::FlushFailed => Self::FlushFailed,
      Self::WriteConflict => Self::WriteConflict,
      Self::ThreadConflict => Self::ThreadConflict,
      Self::ChannelDisconnected => Self::ChannelDisconnected,
      Self::KeyExceeded(e, r) => Self::KeyExceeded(*e, *r),
      Self::ValueExceeded(e, r) => Self::ValueExceeded(*e, *r),
      Self::Panic(err) => Self::Panic(err.clone()),
    }
  }
}

pub type Result<T = ()> = result::Result<T, Error>;
unsafe impl Send for Error {}
unsafe impl Sync for Error {}
