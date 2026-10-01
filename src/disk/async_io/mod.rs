use super::*;

#[cfg(target_os = "linux")]
mod io_uring;
#[cfg(target_os = "linux")]
pub use io_uring::*;

mod task;
pub use task::*;

#[cfg(not(target_os = "linux"))]
mod fallback;
#[cfg(not(target_os = "linux"))]
pub use fallback::*;
