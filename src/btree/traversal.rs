use crate::{
  objects::{TreeHeader, HEADER_POINTER},
  table::TableHandleRef,
  Result,
};

use super::ReadonlyPolicy;

pub fn read_header<Policy: ReadonlyPolicy>(
  policy: &Policy,
  table: &TableHandleRef,
) -> Result<TreeHeader> {
  policy
    .fetch_slot(HEADER_POINTER, table)?
    .for_read()
    .as_ref()
    .deserialize()
}
