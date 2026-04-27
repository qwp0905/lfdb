#[macro_export]
macro_rules! trace {
  ($($arg:tt)+) => {
    if ::log::log_enabled!(::log::Level::Trace) {
      ::log::trace!($($arg)+);
    }
  };
}

#[macro_export]
macro_rules! debug {
  ($($arg:tt)+) => {
    if ::log::log_enabled!(::log::Level::Debug) {
      ::log::debug!($($arg)+);
    }
  };
}

#[macro_export]
macro_rules! info {
  ($($arg:tt)+) => {
    if ::log::log_enabled!(::log::Level::Info) {
      ::log::info!($($arg)+);
    }
  };
}

#[macro_export]
macro_rules! warn {
  ($($arg:tt)+) => {
    if ::log::log_enabled!(::log::Level::Warn) {
      ::log::warn!($($arg)+);
    }
  };
}

#[macro_export]
macro_rules! error {
  ($($arg:tt)+) => {
    if ::log::log_enabled!(::log::Level::Error) {
      ::log::error!($($arg)+);
    }
  };
}
