use std::{collections::VecDeque, ops::Bound};

use crate::{
  blob::{BlobId, BlobLen, BlobOffset},
  cache::ReadonlySlot,
  disk::Pointer,
  objects::{
    BTreeNodeView, DataEntryView, RecordDataView, StaticKey, TreeHeader,
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
      RecordDataView::Data(range) => Some(Self::new(
        BufferedValue::Data(VecRef::refed(slot.clone(), range)),
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
      }));
    }
  }

  pub const fn is_done(&self) -> bool {
    self.0.closed
  }
}

fn find_in_entry<Policy: ReadonlyPolicy>(
  policy: Policy,
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
            if let Some(found) = find_in_entry(&policy, table, p)? {
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

  fn find_value(&self, ptr: Pointer) -> Result<Option<Option<BufferedRecord>>> {
    find_in_entry(&self.policy, &self.table, ptr)
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

struct StackFrame {
  slot: ReadonlySlot,
  pos: usize,
}
impl StackFrame {
  const fn new(slot: ReadonlySlot, pos: usize) -> Self {
    Self { slot, pos }
  }
}
pub struct BTreeRevIter<Policy> {
  policy: Policy,
  table: TableHandleRef,
  buffered: Vec<(VecRef, Option<BufferedRecord>)>,
  stack: Vec<StackFrame>,
  start: Bound<StaticKey>,
  end: Bound<StaticKey>,
  closed: bool,
}
impl<Policy: ReadonlyPolicy> BTreeRevIter<Policy> {
  pub fn open(
    policy: Policy,
    table: &TableHandleRef,
    start: &Bound<StaticKey>,
    end: &Bound<StaticKey>,
  ) -> Result<Self> {
    let ptr = policy
      .fetch_slot(HEADER_POINTER, table)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    let mut stack = Vec::new();
    let mut buffered = Vec::new();
    let (end, closed) =
      Self::descent(&policy, table, &mut buffered, &mut stack, ptr, start, end)?;
    Ok(Self {
      policy,
      table: table.clone(),
      buffered,
      stack,
      start: start.clone(),
      end: Bound::Excluded(end),
      closed,
    })
  }

  fn descent(
    policy: &Policy,
    table: &TableHandleRef,
    buffered: &mut Vec<(VecRef, Option<BufferedRecord>)>,
    stack: &mut Vec<StackFrame>,
    mut ptr: Pointer,
    start: &Bound<StaticKey>,
    end: &Bound<StaticKey>,
  ) -> Result<(StaticKey, bool)> {
    let mut upper = None;
    let mut closed = false;
    loop {
      let slot = policy.fetch_slot(ptr, table)?.for_read();
      match slot.as_ref().view::<BTreeNodeView>()? {
        BTreeNodeView::Internal(node) => {
          let (pos, p) = match end {
            Bound::Included(k) => match node.find_pos(k)? {
              Ok((pos, p)) => (pos, p),
              Err(p) => (node.len() + 1, p),
            },
            Bound::Excluded(k) => match node.find_excluded(k)? {
              Ok((pos, p)) => (pos, p),
              Err(p) => (node.len() + 1, p),
            },
            Bound::Unbounded => match node.get_right() {
              Some((_, p)) => (node.len() + 1, p),
              None => (node.len(), node.last_child()?),
            },
          };

          ptr = p;
          stack.push(StackFrame::new(slot, pos));
        }
        BTreeNodeView::Leaf(node) => {
          if upper.is_none() {
            let top = node.top()?;
            if match start {
              Bound::Included(k) => top <= k,
              Bound::Excluded(k) => top <= k,
              Bound::Unbounded => false,
            } {
              closed = true;
              stack.clear();
            }
            upper = Some(top.to_vec());
          }

          let mut iter = node.range_entries(start, end);
          while let Some(e) = iter.try_next()? {
            if policy.is_visible(e.record.owner, e.record.version) {
              buffered.push((
                VecRef::refed(slot.clone(), e.range),
                BufferedRecord::from(&slot, e.record),
              ));
              continue;
            }

            let Some(p) = e.next else { continue };
            if let Some(found) = find_in_entry(policy, table, p)? {
              buffered.push((VecRef::refed(slot.clone(), e.range), found));
            };
          }

          if let Some((p, high)) = node.get_next_with_key() {
            if match end {
              Bound::Included(key) => high <= key,
              Bound::Excluded(key) => high < key,
              Bound::Unbounded => true,
            } {
              ptr = p;
              continue;
            }
          };

          return Ok((upper.unwrap(), closed));
        }
      }
    }
  }

  fn fill_up(&mut self) -> Result {
    loop {
      if self.closed {
        return Ok(());
      }

      let Some(frame) = self.stack.last_mut() else {
        self.closed = true;
        return Ok(());
      };

      if frame.pos == 0 {
        self.stack.pop();
        continue;
      }
      frame.pos -= 1;

      let node = frame
        .slot
        .as_ref()
        .view::<BTreeNodeView>()?
        .into_internal()?;
      let ptr = node.nth_child(frame.pos)?;
      let (end, closed) = Self::descent(
        &self.policy,
        &self.table,
        &mut self.buffered,
        &mut self.stack,
        ptr,
        &self.start,
        &self.end,
      )?;
      self.end = Bound::Excluded(end);
      self.closed = closed;
      return Ok(());
    }
  }

  fn next_record(&mut self) -> Result<Option<(VecRef, Option<BufferedRecord>)>> {
    loop {
      if let Some((key, found)) = self.buffered.pop() {
        return Ok(Some((key, found)));
      }
      if self.closed {
        return Ok(None);
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
impl<Policy: ReadonlyPolicy> MergeSortable for BTreeRevIter<Policy> {
  fn try_next(&mut self) -> Result<Option<(VecRef, ScannedItem)>> {
    self.next_kv()
  }
}
