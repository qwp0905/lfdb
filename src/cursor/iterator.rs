use std::{collections::VecDeque, ops::Bound};

use crate::{
  blob::{BlobId, BlobLen, BlobOffset},
  cache::ReadonlySlot,
  disk::Pointer,
  objects::{
    BTreeNodeView, DataEntryView, RecordDataView, RecordId, StaticKey, TreeHeader,
    VersionRecordView, HEADER_POINTER,
  },
  table::TableHandleRef,
  wal::TxId,
  Result,
};

use super::{MergeSortable, ReadonlyPolicy, ScannedItem, VecRef};

/**
 * Buffered record payload used by snapshot-oriented iteration.
 *
 * Inline data is kept as bytes, while blob data keeps its existing blob pointer.
 * Blob segments are reclaimed by reference counting and their locations are
 * stable, so snapshot/compaction does not copy blob bytes through this iterator.
 */
pub enum BufferedValue {
  Data(VecRef),
  Blob(BlobId, BlobOffset, BlobLen),
}

struct BufferedRecord {
  data: BufferedValue,
  owner: TxId,
  version: TxId,
  id: RecordId,
}
impl BufferedRecord {
  const fn new(data: BufferedValue, owner: TxId, version: TxId, id: RecordId) -> Self {
    Self {
      data,
      owner,
      version,
      id,
    }
  }

  fn from(slot: &ReadonlySlot, record: VersionRecordView) -> Option<Self> {
    match record.data {
      RecordDataView::Data(range) => Some(Self::new(
        BufferedValue::Data(VecRef::refed(slot.clone(), range)),
        record.owner,
        record.version,
        record.record_id,
      )),
      RecordDataView::Blob(id, offset, len) => Some(Self::new(
        BufferedValue::Blob(id, offset, len),
        record.owner,
        record.version,
        record.record_id,
      )),
      RecordDataView::Tombstone => None,
    }
  }
}

pub struct KVSnapshot {
  pub key: VecRef,
  pub value: BufferedValue,
  pub owner: TxId,
  pub version: TxId,
  pub record_id: RecordId,
}

/**
 * Iterates visible records for snapshot/compaction work.
 *
 * Unlike `BTreeIterator`'s value stream, this wrapper is record-oriented. It
 * preserves the visible record metadata and blob pointer so callers can copy the
 * record itself instead of only its value.
 */
pub struct Snapshotter<Policy>(BTreeIter<Policy>);
impl<Policy: ReadonlyPolicy> Snapshotter<Policy> {
  pub fn open(policy: Policy, table: &TableHandleRef) -> Result<Self> {
    BTreeIter::open(policy, table, &Bound::Unbounded, &Bound::Unbounded).map(Self)
  }

  /**
   * Return the next live visible record for snapshot/compaction.
   *
   * Committed tombstones are intentionally skipped. They are obsolete space and
   * scan-cost overhead, and removing them is one of the reasons this snapshot path
   * exists.
   */
  pub fn next_snapshot(&mut self) -> Result<Option<KVSnapshot>> {
    loop {
      let Some((key, found)) = self.0.next_record()? else {
        return Ok(None);
      };
      let Some(record) = found else {
        continue;
      };

      return Ok(Some(KVSnapshot {
        key,
        value: record.data,
        owner: record.owner,
        version: record.version,
        record_id: record.id,
      }));
    }
  }

  pub const fn is_done(&self) -> bool {
    self.0.closed
  }
}

/**
 * Iterates the visible key/value stream of a B-tree.
 *
 * This iterator is value-oriented: it resolves the visible record for each key
 * and returns only the key plus live value/tombstone needed by merge-sort users.
 * Blob values are materialized through the policy before they are returned.
 */
pub struct BTreeIter<Policy> {
  policy: Policy,
  table: TableHandleRef,
  buffered: VecDeque<(VecRef, Option<BufferedRecord>)>,
  next: Option<Pointer>,
  end: Bound<StaticKey>,
  closed: bool,
}
impl<Policy> BTreeIter<Policy>
where
  Policy: ReadonlyPolicy,
{
  pub fn open(
    policy: Policy,
    table: &TableHandleRef,
    start: &Bound<StaticKey>,
    end: &Bound<StaticKey>,
  ) -> Result<Self> {
    let mut ptr = policy
      .fetch_slot(HEADER_POINTER, table)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    let mut buffered = VecDeque::new();

    loop {
      let slot = policy.fetch_slot(ptr, table)?.for_read();
      match slot.as_ref().view::<BTreeNodeView>()? {
        BTreeNodeView::Internal(node) => match start {
          Bound::Included(k) => ptr = node.find(k)?.unwrap_or_else(|i| i),
          Bound::Excluded(k) => ptr = node.find(k)?.unwrap_or_else(|i| i),
          Bound::Unbounded => ptr = node.first_child()?,
        },
        BTreeNodeView::Leaf(node) => {
          let mut iter = node.range_entries(start, end);
          while let Some(e) = iter.try_next()? {
            if policy.is_visible(e.record.owner, e.record.version) {
              buffered.push_back((
                VecRef::refed(slot.clone(), e.range),
                BufferedRecord::from(&slot, e.record),
              ));
              continue;
            }

            let Some(p) = e.next else { continue };
            if let Some(found) = Self::__find(&policy, table, p)? {
              buffered.push_back((VecRef::refed(slot.clone(), e.range), found));
            };
          }

          let mut next = None;
          if iter.is_completed() {
            next = node.get_next();
          }

          return Ok(Self {
            policy,
            table: table.clone(),
            buffered,
            next,
            end: end.clone(),
            closed: false,
          });
        }
      }
    }
  }

  fn __find(
    policy: &Policy,
    table: &TableHandleRef,
    ptr: Pointer,
  ) -> Result<Option<Option<BufferedRecord>>> {
    let mut next = Some(ptr);

    while let Some(ptr) = next.take() {
      let slot = policy.fetch_slot(ptr, table)?.for_read();
      let entry: DataEntryView = slot.as_ref().view()?;
      if let Some(record) =
        entry.find(|record| policy.is_visible(record.owner, record.version))?
      {
        return Ok(Some(BufferedRecord::from(&slot, record)));
      }

      next = entry.get_next();
    }

    Ok(None)
  }

  fn find_value(&self, ptr: Pointer) -> Result<Option<Option<BufferedRecord>>> {
    Self::__find(&self.policy, &self.table, ptr)
  }

  fn fill_up(&mut self) -> Result {
    debug_assert!(self.buffered.is_empty());

    let Some(ptr) = self.next.take() else {
      self.closed = true;
      return Ok(());
    };

    let slot = self.policy.fetch_slot(ptr, &self.table)?.for_read();
    let node = slot.as_ref().view::<BTreeNodeView>()?.into_leaf()?;

    let mut iter = node.range_entries(&Bound::Unbounded, &self.end);
    while let Some(e) = iter.try_next()? {
      if self.policy.is_visible(e.record.owner, e.record.version) {
        self.buffered.push_back((
          VecRef::refed(slot.clone(), e.range),
          BufferedRecord::from(&slot, e.record),
        ));
        continue;
      }

      let Some(p) = e.next else { continue };
      if let Some(found) = self.find_value(p)? {
        self
          .buffered
          .push_back((VecRef::refed(slot.clone(), e.range), found));
      };
    }

    if iter.is_completed() {
      self.next = node.get_next();
    }
    Ok(())
  }

  fn next_record(&mut self) -> Result<Option<(VecRef, Option<BufferedRecord>)>> {
    loop {
      if self.closed {
        return Ok(None);
      }

      if let Some((key, found)) = self.buffered.pop_front() {
        return Ok(Some((key, found)));
      }

      self.fill_up()?;
    }
  }

  fn next_kv(&mut self) -> Result<Option<(VecRef, ScannedItem)>> {
    let Some((key, found)) = self.next_record()? else {
      return Ok(None);
    };
    let Some(record) = found else {
      return Ok(Some((key, ScannedItem::Deleted)));
    };
    match record.data {
      BufferedValue::Data(data) => Ok(Some((key, ScannedItem::Present(data)))),
      BufferedValue::Blob(id, offset, len) => Ok(Some((
        key,
        ScannedItem::Present(VecRef::copied(self.policy.read_blob(id, offset, len)?)),
      ))),
    }
  }
}
impl<Policy: ReadonlyPolicy> MergeSortable for BTreeIter<Policy> {
  fn try_next(&mut self) -> Result<Option<(VecRef, ScannedItem)>> {
    self.next_kv()
  }
}
