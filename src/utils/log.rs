#[clippy::format_args]
macro_rules! trace {
  ($($arg:tt)+) => {
    if ::log::log_enabled!(::log::Level::Trace) {
      ::log::trace!($($arg)+);
    }
  };
}
pub(crate) use trace;

#[clippy::format_args]
macro_rules! debug {
  ($($arg:tt)+) => {
    if ::log::log_enabled!(::log::Level::Debug) {
      ::log::debug!($($arg)+);
    }
  };
}
pub(crate) use debug;

#[clippy::format_args]
macro_rules! info {
  ($($arg:tt)+) => {
    if ::log::log_enabled!(::log::Level::Info) {
      ::log::info!($($arg)+);
    }
  };
}
pub(crate) use info;

#[clippy::format_args]
macro_rules! warn_ {
  ($($arg:tt)+) => {
    if ::log::log_enabled!(::log::Level::Warn) {
      ::log::warn!($($arg)+);
    }
  };
}
pub(crate) use warn_ as warn;

#[clippy::format_args]
macro_rules! error {
  ($($arg:tt)+) => {
    if ::log::log_enabled!(::log::Level::Error) {
      ::log::error!($($arg)+);
    }
  };
}
pub(crate) use error;
