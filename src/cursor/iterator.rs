use std::{collections::VecDeque, ops::Bound};

use crate::{
  cache::ReadonlySlot, disk::Pointer, table::TableHandleRef, wal::TxId, Result,
};

use crossbeam::epoch::pin;

use super::{
  BTreeNodeView, BlobId, BlobLen, BlobOffset, DataEntryView, MergeSortable,
  ReadonlyPolicy, RecordDataView, StaticKey, TreeHeader, VecRef, VersionRecordView,
  HEADER_POINTER,
};

pub enum BufferedValue {
  Data(VecRef),
  Blob(BlobId, BlobOffset, BlobLen),
}

struct BufferedRecord {
  data: BufferedValue,
  owner: TxId,
  version: TxId,
}
impl BufferedRecord {
  const fn new(data: BufferedValue, owner: TxId, version: TxId) -> Self {
    Self {
      data,
      owner,
      version,
    }
  }

  fn from(slot: &ReadonlySlot, record: VersionRecordView) -> Option<Self> {
    match record.data {
      RecordDataView::Data(s, e) => Some(Self::new(
        BufferedValue::Data(VecRef::refed(slot.page(), s, e)),
        record.owner,
        record.version,
      )),
      RecordDataView::Blob(id, offset, len) => Some(Self::new(
        BufferedValue::Blob(id, offset, len),
        record.owner,
        record.version,
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
}

pub struct Snapshotter<Policy>(BTreeIterator<Policy>);
impl<Policy: ReadonlyPolicy> Snapshotter<Policy> {
  pub fn open(policy: Policy, table: &TableHandleRef) -> Result<Self> {
    BTreeIterator::open(policy, table, &Bound::Unbounded, &Bound::Unbounded).map(Self)
  }

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
      }));
    }
  }

  pub const fn is_done(&self) -> bool {
    self.0.closed
  }
}

pub struct BTreeIterator<Policy> {
  policy: Policy,
  table: TableHandleRef,
  buffered: VecDeque<(VecRef, Option<BufferedRecord>)>,
  next: Option<Pointer>,
  end: Bound<StaticKey>,
  closed: bool,
}
impl<Policy> BTreeIterator<Policy>
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
        BTreeNodeView::Internal(node) => match &start {
          Bound::Included(k) => ptr = node.find(k)?.unwrap_or_else(|i| i),
          Bound::Excluded(k) => ptr = node.find(k)?.unwrap_or_else(|i| i),
          Bound::Unbounded => ptr = node.first_child()?,
        },
        BTreeNodeView::Leaf(node) => {
          let mut iter = node.range_entries(start, end);
          while let Some((s, e, record, p)) = iter.try_next()? {
            if policy.is_visible(record.owner, record.version) {
              buffered.push_back((
                VecRef::refed(slot.page(), s, e),
                BufferedRecord::from(&slot, record),
              ));
              continue;
            }

            if let Some(found) = Self::__find(&policy, table, p)? {
              buffered.push_back((VecRef::refed(slot.page(), s, e), found));
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

    let mut _guard = None;
    while let Some(ptr) = next.take() {
      let new_guard = pin();
      let slot = policy.fetch_slot(ptr, table)?.for_read();
      let entry: DataEntryView = slot.as_ref().view()?;

      if let Some(record) =
        entry.find(|record| policy.is_visible(record.owner, record.version))?
      {
        return Ok(Some(BufferedRecord::from(&slot, record)));
      }

      next = entry.get_next();
      _guard = Some(new_guard);
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
    while let Some((s, e, record, p)) = iter.try_next()? {
      if self.policy.is_visible(record.owner, record.version) {
        self.buffered.push_back((
          VecRef::refed(slot.page(), s, e),
          BufferedRecord::from(&slot, record),
        ));
        continue;
      }

      if let Some(found) = self.find_value(p)? {
        self
          .buffered
          .push_back((VecRef::refed(slot.page(), s, e), found));
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

  fn next_kv(&mut self) -> Result<Option<(VecRef, Option<VecRef>)>> {
    let Some((key, found)) = self.next_record()? else {
      return Ok(None);
    };
    let Some(record) = found else {
      return Ok(Some((key, None)));
    };
    match record.data {
      BufferedValue::Data(data) => Ok(Some((key, Some(data)))),
      BufferedValue::Blob(id, offset, len) => Ok(Some((
        key,
        Some(VecRef::copied(self.policy.read_blob(id, offset, len)?)),
      ))),
    }
  }
}
impl<Policy: ReadonlyPolicy> MergeSortable for BTreeIterator<Policy> {
  fn try_next(&mut self) -> Result<Option<(VecRef, Option<VecRef>)>> {
    self.next_kv()
  }
}
