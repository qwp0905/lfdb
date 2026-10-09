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
  loop {
    if let Some(slot) = policy.fetch_slot(HEADER_POINTER, table)? {
      return slot.for_read().as_ref().deserialize();
    };
  }
}
