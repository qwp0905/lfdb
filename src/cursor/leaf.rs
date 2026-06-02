use std::{mem::replace, ops::Bound};

use super::{
  RecordData, RecordDataView, StaticKey, StaticKeyRef, VersionRecord, VersionRecordView,
  MAX_KEY,
};
use crate::{
  disk::{Page, PageScanner, PageWriter, Pointer, POINTER_BYTES},
  serialize::SERIALIZABLE_BYTES,
  wal::TX_ID_BYTES,
  Result,
};

// Maximum inline value size for a leaf entry.
pub const LARGE_VALUE: usize = ((SERIALIZABLE_BYTES - (1 + POINTER_BYTES + 2)) >> 1)
  - (MAX_KEY + POINTER_BYTES + 2 + (TX_ID_BYTES << 1) + 1 + 2);

#[derive(Debug)]
pub struct LeafEntry {
  key: StaticKey,
  record: VersionRecord,
  next: Pointer,
}
impl LeafEntry {
  const fn new(key: StaticKey, record: VersionRecord, next: Pointer) -> Self {
    Self { key, record, next }
  }

  const fn bytes_len(&self) -> usize {
    self.key.len() + 2 + POINTER_BYTES + self.record.byte_len()
  }
}

/**
 * B+tree leaf node. Leaf nodes are linked in key order via next, set when this
 * node is split to chain the new right sibling into the list.
 */
#[derive(Debug)]
pub struct LeafNode {
  entries: Vec<LeafEntry>,
  next: Option<Pointer>,
}
impl LeafNode {
  pub const fn empty() -> Self {
    Self {
      entries: Vec::new(),
      next: None,
    }
  }
  pub const fn new(entries: Vec<LeafEntry>, next: Option<Pointer>) -> Self {
    Self { entries, next }
  }

  pub fn write_at(&self, writer: &mut PageWriter) -> Result {
    writer.write_u64(self.next.unwrap_or(0))?;
    writer.write_u16(self.entries.len() as u16)?;
    for entry in &self.entries {
      writer.write_u16(entry.key.len() as u16)?;
      writer.write(&entry.key)?;
      entry.record.serialize_to(writer)?;
      writer.write_u64(entry.next)?;
    }
    Ok(())
  }

  pub fn from_scanner(scanner: &mut PageScanner) -> Result<Self> {
    let next = scanner.read_u64()?;
    let len = scanner.read_u16()? as usize;
    let mut entries = Vec::with_capacity(len);
    for _ in 0..len {
      let l = scanner.read_u16()? as usize;
      let key = scanner.read_n(l)?.to_vec();
      let record = VersionRecord::deserialize_from(scanner)?;
      let next = scanner.read_u64()?;
      entries.push(LeafEntry::new(key, record, next))
    }
    Ok(Self::new(entries, (next != 0).then(|| next)))
  }

  pub const fn set_next(&mut self, pointer: Pointer) -> Option<Pointer> {
    self.next.replace(pointer)
  }

  #[inline]
  fn bytes_len(&self) -> usize {
    1 + POINTER_BYTES + 2 + self.entries.iter().map(|e| e.bytes_len()).sum::<usize>()
  }

  pub fn split_if_needed(&mut self) -> Option<LeafNode> {
    let bytes_len = self.bytes_len();
    if bytes_len <= SERIALIZABLE_BYTES {
      return None;
    }

    let mut sum = 1 + POINTER_BYTES + 2;
    let mut mid = 0;
    while sum <= bytes_len >> 1 {
      sum += self.entries[mid].bytes_len();
      mid += 1;
    }

    let split = Self::new(self.entries.split_off(mid), self.next.take());
    debug_assert!(self.bytes_len() <= SERIALIZABLE_BYTES);
    debug_assert!(split.bytes_len() <= SERIALIZABLE_BYTES);
    Some(split)
  }

  pub fn replace_at(&mut self, index: usize, record: VersionRecord) -> VersionRecord {
    replace(&mut self.entries[index].record, record)
  }
  pub fn insert_at(
    &mut self,
    index: usize,
    key: StaticKey,
    record: VersionRecord,
    pointer: Pointer,
  ) {
    self
      .entries
      .insert(index, LeafEntry::new(key, record, pointer));
  }

  pub fn top(&self) -> &StaticKey {
    &self.entries[0].key
  }
}

/**
 * Result of a leaf node key lookup.
 * Move is the B-link tree right-move: the key falls beyond this node's range,
 * so the caller must follow the next pointer to the right sibling — the same
 * mechanism used at the internal level when a search key >= high key.
 */
pub enum NodeFindResult<'a> {
  Found(usize, &'a VersionRecordView, Pointer),
  Move(Pointer),
  NotFound(usize),
}

#[derive(Debug)]
struct LeafEntryView {
  key_start: usize,
  key_end: usize,
  record: VersionRecordView,
  next: Pointer,
}
impl LeafEntryView {
  const fn new(
    key_start: usize,
    key_end: usize,
    record: VersionRecordView,
    next: Pointer,
  ) -> Self {
    Self {
      key_start,
      key_end,
      record,
      next,
    }
  }
}

#[derive(Debug)]
pub struct LeafNodeView<'a> {
  page: &'a Page,
  entries: Vec<LeafEntryView>,
  next: Option<Pointer>,
}
impl<'a> LeafNodeView<'a> {
  const fn new(
    page: &'a Page,
    entries: Vec<LeafEntryView>,
    next: Option<Pointer>,
  ) -> Self {
    Self {
      page,
      entries,
      next,
    }
  }
  pub fn from_scanner(page: &'a Page, scanner: &mut PageScanner<'a>) -> Result<Self> {
    let next = scanner.read_u64()?;
    let len = scanner.read_u16()? as usize;
    let mut entries = Vec::with_capacity(len);
    for _ in 0..len {
      let l = scanner.read_u16()? as usize;
      let offset = scanner.advance(l)?;
      let record = VersionRecordView::deserialize_from(scanner)?;
      let ptr = scanner.read_u64()?;
      entries.push(LeafEntryView::new(offset, offset + l, record, ptr));
    }
    Ok(Self::new(page, entries, (next != 0).then(|| next)))
  }

  pub fn find(&self, key: StaticKeyRef) -> NodeFindResult<'_> {
    match self.binary_search(key) {
      Ok(i) => NodeFindResult::Found(i, &self.entries[i].record, self.entries[i].next),
      Err(i) => {
        if i == self.entries.len() {
          if let Some(p) = self.next {
            return NodeFindResult::Move(p);
          }
        };

        NodeFindResult::NotFound(i)
      }
    }
  }

  #[inline]
  fn binary_search(&self, key: StaticKeyRef) -> std::result::Result<usize, usize> {
    self
      .entries
      .binary_search_by(|e| (self.page.range(e.key_start..e.key_end)).cmp(key))
  }

  pub fn writable(self) -> LeafNode {
    let mut entries = Vec::with_capacity(self.entries.len() + 1);
    for e in self.entries {
      entries.push(LeafEntry::new(
        self.page.copy_range(e.key_start..e.key_end),
        VersionRecord::new(
          e.record.owner,
          e.record.version,
          match e.record.data {
            RecordDataView::Data(s, e) => RecordData::Data(self.page.copy_range(s..e)),
            RecordDataView::Chunked(pointers) => RecordData::Chunked(pointers),
            RecordDataView::Tombstone => RecordData::Tombstone,
          },
        ),
        e.next,
      ))
    }
    LeafNode::new(entries, self.next)
  }

  pub fn get_entries_while<'b: 'a>(
    &self,
    end: &'b Bound<StaticKey>,
  ) -> impl Iterator<Item = (usize, usize, &'_ VersionRecordView, Pointer)> + '_ {
    let e = match end {
      Bound::Included(k) => self.binary_search(k).map(|i| i + 1).unwrap_or_else(|i| i),
      Bound::Excluded(k) => self.binary_search(k).unwrap_or_else(|i| i),
      Bound::Unbounded => self.len(),
    };
    self.get_entries().take(e)
  }

  pub fn get_entries(
    &self,
  ) -> impl Iterator<Item = (usize, usize, &'_ VersionRecordView, Pointer)> + '_ {
    self
      .entries
      .iter()
      .map(|e| (e.key_start, e.key_end, &e.record, e.next))
  }

  pub const fn get_next(&self) -> Option<Pointer> {
    self.next
  }

  pub const fn len(&self) -> usize {
    self.entries.len()
  }
}
