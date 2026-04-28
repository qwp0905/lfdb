use std::sync::Arc;

use crate::{
  cache::WritableSlot,
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
    slot: &mut WritableSlot<'_>,
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
      .append_insert(tx_id, table_id, ptr, page.copy_n(byte_len))
  }

  pub fn log_multi<T, R>(
    &self,
    tx_id: TxId,
    table_id: TableId,
    slot1: &mut WritableSlot<'_>,
    data1: &T,
    slot2: &mut WritableSlot<'_>,
    data2: &R,
  ) -> Result
  where
    T: Serializable,
    R: Serializable,
  {
    let ptr1 = slot1.get_pointer();
    let page = slot1.as_mut();
    let byte_len = page.serialize_from(data1)?;
    let data1 = page.copy_n(byte_len);

    let ptr2 = slot2.get_pointer();
    let page = slot2.as_mut();
    let byte_len = page.serialize_from(data2)?;
    let data2 = page.copy_n(byte_len);

    self
      .wal
      .append_multi(tx_id, table_id, ptr1, data1, ptr2, data2)
  }
}
