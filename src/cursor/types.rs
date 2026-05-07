use crate::utils::InlineVec;

const THRESHOLD: usize = 24;

pub type Key = InlineVec<u8, THRESHOLD>;
pub type KeyRef<'a> = &'a [u8];
