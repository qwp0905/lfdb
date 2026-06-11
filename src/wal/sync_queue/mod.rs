use std::io::Result as IOResult;

use super::FsyncResult;
use crate::Result;

pub type SegmentGeneration = u32;

#[cfg(not(target_os = "linux"))]
mod cvar;
#[cfg(not(target_os = "linux"))]
pub use cvar::*;

#[cfg(target_os = "linux")]
mod futex;
#[cfg(target_os = "linux")]
pub use futex::*;
