use std::sync::Arc;

use crate::{
  cache::RefedSlot,
  error::Result,
  serialize::{Serializable, SerializeFrom},
  table::TableId,
  wal::{TxId, WAL},
};

/**
 * Serializes data into a page slot and writes only the used bytes to the WAL —
 * logging the full page would waste WAL space. copy_n captures only the written
 * portion so the WAL record is as compact as the data allows.
 *
 * Does not implement Drop — WAL lifetime is managed externally.
 * Used by the orchestrator, and GC.
 */
pub struct PageRecorder {
  wal: Arc<WAL>,
}
impl PageRecorder {
  #[inline]
  pub const fn new(wal: Arc<WAL>) -> Self {
    Self { wal }
  }
  #[inline]
  pub fn serialize_and_log<T>(
    &self,
    tx_id: TxId,
    table_id: TableId,
    slot: &mut RefedSlot,
    data: &T,
  ) -> Result
  where
    T: Serializable,
  {
    let ptr = slot.get_pointer();
    let page = slot.as_mut();
    let byte_len = page.serialize_from(data)?;
    self
      .wal
      .append_insert(tx_id, table_id, ptr, page.copy_range(0..byte_len))
  }
}
