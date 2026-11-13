use std::{collections::VecDeque, ops::Bound};

use crate::{
  cache::ReadonlySlot,
  disk::Pointer,
  objects::{BTreeNodeView, RecordDataView, StaticKey, StaticKeyRef, VersionRecordView},
  page::VecRef,
  table::TableHandleRef,
  wal::TxId,
  Result,
};

use super::{
  find_record_in_entry, read_header, KVSnapshot, MergeSortable, ReadonlyPolicy,
  ScannedItem, SnapshotValue,
};

struct BufferedRecord {
  data: SnapshotValue,
  owner: TxId,
  version: TxId,
}
impl BufferedRecord {
  const fn new(data: SnapshotValue, owner: TxId, version: TxId) -> Self {
    Self {
      data,
      owner,
      version,
    }
  }

  fn find<Policy: ReadonlyPolicy>(
    policy: Policy,
    ptr: Pointer,
    table: &TableHandleRef,
  ) -> Result<Option<Option<Self>>> {
    let Some((slot, record)) = find_record_in_entry(&policy, ptr, table)? else {
      return Ok(None);
    };
    Ok(Some(Self::from(&slot, record)))
  }

  fn from(slot: &ReadonlySlot, record: VersionRecordView) -> Option<Self> {
    match record.data {
      RecordDataView::Data(range) => Some(Self::new(
        SnapshotValue::Data(slot.create_ref(range)),
        record.owner,
        record.version,
      )),
      RecordDataView::Blob(id, offset, len) => Some(Self::new(
        SnapshotValue::Blob(id, offset, len),
        record.owner,
        record.version,
      )),
      RecordDataView::Tombstone => None,
    }
  }
}

type BufferedEntry = (VecRef, Option<BufferedRecord>);

struct BTreeIterCore<Policy> {
  policy: Policy,
  table: TableHandleRef,
}
impl<Policy: ReadonlyPolicy> BTreeIterCore<Policy> {
  fn new(policy: Policy, table: &TableHandleRef) -> Self {
    Self {
      policy,
      table: table.clone(),
    }
  }

  fn get_root(&self) -> Result<Pointer> {
    Ok(read_header(&self.policy, &self.table)?.get_root())
  }

  fn descend(&self, start: &Bound<StaticKey>) -> Result<Pointer> {
    let mut ptr = self.get_root()?;
    loop {
      let slot = self.policy.fetch_slot(ptr, &self.table)?.for_read();
      let BTreeNodeView::Internal(node) = slot.as_ref().view::<BTreeNodeView>()? else {
        return Ok(ptr);
      };
      ptr = match start {
        Bound::Included(k) | Bound::Excluded(k) => node.find(k)?.unwrap_or_else(|i| i),
        Bound::Unbounded => node.first_child()?,
      };
    }
  }

  fn read_leaf(
    &self,
    ptr: Pointer,
    start: &Bound<StaticKey>,
    end: &Bound<StaticKey>,
    buffered: &mut VecDeque<BufferedEntry>,
  ) -> Result<Option<Pointer>> {
    let slot = self.policy.fetch_slot(ptr, &self.table)?.for_read();
    let node = slot.as_ref().view::<BTreeNodeView>()?.into_leaf()?;

    let mut iter = node.range_entries(start, end);
    while let Some(e) = iter.try_next()? {
      if self.policy.is_visible(e.record.owner, e.record.version) {
        buffered.push_back((
          slot.create_ref(e.range),
          BufferedRecord::from(&slot, e.record),
        ));
        continue;
      }

      let Some(p) = e.next else { continue };
      if let Some(record) = BufferedRecord::find(&self.policy, p, &self.table)? {
        buffered.push_back((slot.create_ref(e.range), record));
      };
    }

    Ok(node.get_next().filter(|_| iter.is_completed()))
  }

  fn read_leaf_rev(
    &self,
    start: &Bound<StaticKey>,
    end: &Bound<StaticKey>,
    scan: &mut RevIterScanState,
  ) -> Result<Option<StaticKey>> {
    loop {
      let Some(frame) = scan.stack.last_mut() else {
        return Ok(None);
      };

      if frame.pos == 0 {
        scan.stack.pop();
        continue;
      }
      frame.pos -= 1;

      let node = frame
        .slot
        .as_ref()
        .view::<BTreeNodeView>()?
        .into_internal()?;
      let ptr = node.nth_child(frame.pos)?;
      return self.descend_rev(ptr, start, end, scan).map(Some);
    }
  }

  fn descend_rev(
    &self,
    mut ptr: Pointer,
    start: &Bound<StaticKey>,
    end: &Bound<StaticKey>,
    scan: &mut RevIterScanState,
  ) -> Result<StaticKey> {
    let mut upper = None;
    loop {
      let slot = self.policy.fetch_slot(ptr, &self.table)?.for_read();
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
          scan.stack.push(StackFrame::new(slot, pos));
        }
        BTreeNodeView::Leaf(node) => {
          if upper.is_none() {
            let top = node.top()?;
            if match start {
              Bound::Included(k) => top <= k.as_slice(),
              Bound::Excluded(k) => top <= k.as_slice(),
              Bound::Unbounded => false,
            } {
              scan.stack.clear();
            }
            upper = Some(top.to_vec());
          }

          let mut iter = node.range_entries(start, end);
          while let Some(e) = iter.try_next()? {
            if self.policy.is_visible(e.record.owner, e.record.version) {
              scan.buffered.push((
                slot.create_ref(e.range),
                BufferedRecord::from(&slot, e.record),
              ));
              continue;
            }

            let Some(p) = e.next else { continue };
            if let Some(found) = BufferedRecord::find(&self.policy, p, &self.table)? {
              scan.buffered.push((slot.create_ref(e.range), found));
            };
          }

          if let Some((p, high)) = node.get_next_with_key() {
            if match end {
              Bound::Included(key) => high <= key.as_slice(),
              Bound::Excluded(key) => high < key.as_slice(),
              Bound::Unbounded => true,
            } {
              ptr = p;
              continue;
            }
          };

          return Ok(upper.unwrap());
        }
      }
    }
  }
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
  pub fn open(
    policy: Policy,
    table: &TableHandleRef,
    from: Option<StaticKeyRef>,
  ) -> Self {
    let start = from
      .map(|key| Bound::Included(key.to_vec()))
      .unwrap_or(Bound::Unbounded);
    Self(BTreeIter::new(policy, table, &start, &Bound::Unbounded))
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
    matches!(self.0.state, IterState::Done)
  }
}

enum IterState {
  /**
   * Nothing has been read from the tree yet.
   */
  Pending,
  /**
   * A leaf has been reached, but no key at or after `start` has been met yet.
   * `leaf` is the next leaf to read.
   */
  Seeking { leaf: Pointer },
  /**
   * A key at or after `start` has been met. `buffered` holds read entries not yet
   * returned, and `next` is the right sibling still to read, `None` once `end` or
   * the tail of the leaf chain has been reached.
   */
  Scanning {
    buffered: VecDeque<BufferedEntry>,
    next: Option<Pointer>,
  },
  /**
   * Every key in range has been returned.
   */
  Done,
}

/**
 * Iterates the visible key/value stream of a B-tree.
 *
 * This iterator is value-oriented: it resolves the visible record for each key
 * and returns only the key plus live value/tombstone needed by merge-sort users.
 * Blob values are materialized through the policy before they are returned.
 */
pub struct BTreeIter<Policy> {
  core: BTreeIterCore<Policy>,
  start: Bound<StaticKey>,
  end: Bound<StaticKey>,
  state: IterState,
}
impl<Policy> BTreeIter<Policy>
where
  Policy: ReadonlyPolicy,
{
  pub fn new(
    policy: Policy,
    table: &TableHandleRef,
    start: &Bound<StaticKey>,
    end: &Bound<StaticKey>,
  ) -> Self {
    Self {
      core: BTreeIterCore::new(policy, table),
      start: start.clone(),
      end: end.clone(),
      state: IterState::Pending,
    }
  }

  fn next_record(&mut self) -> Result<Option<BufferedEntry>> {
    loop {
      match &mut self.state {
        IterState::Done => return Ok(None),
        IterState::Pending => {
          let leaf = self.core.descend(&self.start)?;
          self.state = IterState::Seeking { leaf };
        }
        IterState::Seeking { leaf } => {
          let mut buffered = VecDeque::new();
          let next = self
            .core
            .read_leaf(*leaf, &self.start, &self.end, &mut buffered)?;
          match (buffered.is_empty(), next) {
            (false, next) => self.state = IterState::Scanning { buffered, next },
            (true, Some(next)) => *leaf = next,
            (true, None) => self.state = IterState::Done,
          }
        }
        IterState::Scanning { buffered, next } => {
          if let Some(entry) = buffered.pop_front() {
            return Ok(Some(entry));
          }
          let Some(ptr) = *next else {
            self.state = IterState::Done;
            continue;
          };

          *next = self
            .core
            .read_leaf(ptr, &Bound::Unbounded, &self.end, buffered)?;
        }
      }
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
      SnapshotValue::Data(data) => Ok(Some((key, ScannedItem::Present(data)))),
      SnapshotValue::Blob(id, offset, len) => Ok(Some((
        key,
        ScannedItem::Present(VecRef::copied(
          self.core.policy.read_blob(id, offset, len)?,
        )),
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

struct RevIterScanState {
  buffered: Vec<BufferedEntry>,
  stack: Vec<StackFrame>,
}
impl RevIterScanState {
  const fn new() -> Self {
    Self {
      buffered: Vec::new(),
      stack: Vec::new(),
    }
  }
}

enum RevIterState {
  /**
   * Nothing has been read from the tree yet.
   */
  Pending { end: Bound<StaticKey> },
  /**
   * At least one leaf has been read. `end` excludes every key already read.
   */
  Scanning {
    scan: RevIterScanState,
    end: Bound<StaticKey>,
  },
  /**
   * Every key in range has been returned.
   */
  Done,
}

pub struct BTreeRevIter<Policy> {
  core: BTreeIterCore<Policy>,
  start: Bound<StaticKey>,
  state: RevIterState,
}
impl<Policy: ReadonlyPolicy> BTreeRevIter<Policy> {
  pub fn new(
    policy: Policy,
    table: &TableHandleRef,
    start: &Bound<StaticKey>,
    end: &Bound<StaticKey>,
  ) -> Self {
    Self {
      core: BTreeIterCore::new(policy, table),
      start: start.clone(),
      state: RevIterState::Pending { end: end.clone() },
    }
  }

  fn next_record(&mut self) -> Result<Option<BufferedEntry>> {
    loop {
      match &mut self.state {
        RevIterState::Done => return Ok(None),
        RevIterState::Pending { end } => {
          let root = self.core.get_root()?;
          let mut scan = RevIterScanState::new();
          let upper = self.core.descend_rev(root, &self.start, end, &mut scan)?;
          self.state = RevIterState::Scanning {
            scan,
            end: Bound::Excluded(upper),
          };
        }
        RevIterState::Scanning { scan, end } => {
          if let Some(entry) = scan.buffered.pop() {
            return Ok(Some(entry));
          }

          let Some(upper) = self.core.read_leaf_rev(&self.start, end, scan)? else {
            self.state = RevIterState::Done;
            return Ok(None);
          };
          *end = Bound::Excluded(upper);
        }
      };
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
      SnapshotValue::Data(data) => Ok(Some((key, ScannedItem::Present(data)))),
      SnapshotValue::Blob(id, offset, len) => Ok(Some((
        key,
        ScannedItem::Present(VecRef::copied(
          self.core.policy.read_blob(id, offset, len)?,
        )),
      ))),
    }
  }
}
impl<Policy: ReadonlyPolicy> MergeSortable for BTreeRevIter<Policy> {
  fn try_next(&mut self) -> Result<Option<(VecRef, ScannedItem)>> {
    self.next_kv()
  }
}
