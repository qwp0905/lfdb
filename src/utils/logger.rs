use std::{panic::RefUnwindSafe, sync::Arc};

#[allow(unused)]
#[derive(Clone, Copy)]
pub enum LogLevel {
  Trace = 0,
  Debug = 1,
  Info = 2,
  Warn = 3,
  Error = 4,
  Fatal = 5,
}
impl LogLevel {
  pub fn to_str(&self) -> &str {
    match self {
      LogLevel::Trace => "trace",
      LogLevel::Debug => "debug",
      LogLevel::Info => "info",
      LogLevel::Warn => "warn",
      LogLevel::Error => "error",
      LogLevel::Fatal => "fatal",
    }
  }
}

/**
 * Implement this trait to receive log output from the engine.
 * NoneLogger is provided as a no-op default.
 */
pub trait Logger: Send + Sync + RefUnwindSafe {
  fn log(&self, level: LogLevel, msg: &[u8]);
}
pub struct LogFilter {
  level: LogLevel,
  logger: Arc<dyn Logger>,
}
impl LogFilter {
  pub fn new(level: LogLevel, logger: Arc<dyn Logger>) -> Self {
    Self { level, logger }
  }

  #[inline(always)]
  fn log<T, F>(&self, level: LogLevel, msg: F)
  where
    T: AsRef<[u8]>,
    F: FnOnce() -> T,
  {
    if self.level as isize <= level as isize {
      self.logger.log(level, msg().as_ref())
    }
  }
  #[inline]
  pub fn info<T: AsRef<[u8]>, F: FnOnce() -> T>(&self, msg: F) {
    self.log(LogLevel::Info, msg)
  }
  #[inline]
  pub fn warn<T: AsRef<[u8]>, F: FnOnce() -> T>(&self, msg: F) {
    self.log(LogLevel::Warn, msg)
  }
  #[inline]
  pub fn error<T: AsRef<[u8]>, F: FnOnce() -> T>(&self, msg: F) {
    self.log(LogLevel::Error, msg)
  }
  #[inline]
  pub fn fatal<T: AsRef<[u8]>, F: FnOnce() -> T>(&self, msg: F) {
    self.log(LogLevel::Fatal, msg)
  }
  #[inline]
  pub fn debug<T: AsRef<[u8]>, F: FnOnce() -> T>(&self, msg: F) {
    self.log(LogLevel::Debug, msg)
  }
  #[inline]
  pub fn trace<T: AsRef<[u8]>, F: FnOnce() -> T>(&self, msg: F) {
    self.log(LogLevel::Trace, msg)
  }
}
impl Clone for LogFilter {
  fn clone(&self) -> Self {
    Self {
      level: self.level,
      logger: self.logger.clone(),
    }
  }
}

pub struct NoneLogger;
impl Logger for NoneLogger {
  fn log(&self, _: LogLevel, _: &[u8]) {}
}
