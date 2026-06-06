use std::{mem::replace, ops::Bound};

use super::{StaticKey, StaticKeyRef, VersionRecord, VersionRecordView, MAX_KEY};
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
pub enum NodeFindResult {
  Found(usize, VersionRecordView, Pointer),
  Move(Pointer),
  NotFound(usize),
}

#[derive(Debug)]
pub struct LeafNodeView<'a> {
  page: &'a Page,
  offset: usize,
  len: usize,
  next: Option<Pointer>,
}
impl<'a> LeafNodeView<'a> {
  const fn new(page: &'a Page, offset: usize, len: usize, next: Option<Pointer>) -> Self {
    Self {
      page,
      offset,
      len,
      next,
    }
  }
  pub fn from_scanner(page: &'a Page, scanner: &mut PageScanner<'a>) -> Result<Self> {
    let next = scanner.read_u64()?;
    let len = scanner.read_u16()? as usize;
    let offset = scanner.advance(0)?;
    Ok(Self::new(page, offset, len, (next != 0).then(|| next)))
  }

  pub fn find(&self, key: StaticKeyRef) -> Result<NodeFindResult> {
    let mut scanner = self.page.scanner();
    scanner.advance(self.offset).unwrap();

    for i in 0..self.len {
      let l = scanner.read_u16()? as usize;
      let offset = scanner.advance(l)?;
      let record = VersionRecordView::deserialize_from(&mut scanner)?;
      let ptr = scanner.read_u64()?;

      let k = self.page.range(offset..offset + l);
      if k < key {
        continue;
      } else if k == key {
        return Ok(NodeFindResult::Found(i, record, ptr));
      } else {
        return Ok(NodeFindResult::NotFound(i));
      }
    }

    Ok(
      self
        .next
        .map(NodeFindResult::Move)
        .unwrap_or_else(|| NodeFindResult::NotFound(self.len)),
    )
  }

  pub fn writable(self) -> Result<LeafNode> {
    let mut scanner = self.page.scanner();
    scanner.advance(self.offset).unwrap();

    let mut entries = Vec::with_capacity(self.len + 1);
    for _ in 0..self.len {
      let l = scanner.read_u16()? as usize;
      let key = scanner.read_n(l)?.to_vec();
      let record = VersionRecord::deserialize_from(&mut scanner)?;
      let next = scanner.read_u64()?;
      entries.push(LeafEntry::new(key, record, next))
    }

    Ok(LeafNode::new(entries, self.next))
  }

  pub fn get_entries(&self) -> LeafNodeIter<'_> {
    let mut scanner = self.page.scanner();
    scanner.advance(self.offset).unwrap();
    LeafNodeIter {
      scanner,
      page: self.page,
      start: &Bound::Unbounded,
      end: &Bound::Unbounded,
      pos: 0,
      len: self.len,
      closed: false,
    }
  }

  pub fn range_entries(
    &'a self,
    start: &'a Bound<StaticKey>,
    end: &'a Bound<StaticKey>,
  ) -> LeafNodeIter<'a> {
    let mut scanner = self.page.scanner();
    scanner.advance(self.offset).unwrap();
    LeafNodeIter {
      scanner,
      page: self.page,
      start,
      end,
      pos: 0,
      len: self.len,
      closed: false,
    }
  }

  pub const fn get_next(&self) -> Option<Pointer> {
    self.next
  }
}

pub struct LeafNodeIter<'a> {
  scanner: PageScanner<'a>,
  page: &'a Page,
  start: &'a Bound<StaticKey>,
  end: &'a Bound<StaticKey>,
  pos: usize,
  len: usize,
  closed: bool,
}
impl<'a> LeafNodeIter<'a> {
  pub fn is_completed(&self) -> bool {
    self.pos == self.len
  }
  pub fn try_next(
    &mut self,
  ) -> Result<Option<(usize, usize, VersionRecordView, Pointer)>> {
    loop {
      if self.closed {
        return Ok(None);
      }
      if self.is_completed() {
        self.closed = true;
        return Ok(None);
      }

      let l = self.scanner.read_u16()? as usize;
      let offset = self.scanner.advance(l)?;
      let record = VersionRecordView::deserialize_from(&mut self.scanner)?;
      let ptr = self.scanner.read_u64()?;

      let key = self.page.range(offset..offset + l);
      match self.start {
        Bound::Included(k) if k.as_slice() > key => {
          self.pos += 1;
          continue;
        }
        Bound::Excluded(k) if k.as_slice() >= key => {
          self.pos += 1;
          continue;
        }
        _ => {}
      }

      let result = (offset, offset + l, record, ptr);
      match self.end {
        Bound::Included(k) if k.as_slice() >= key => {
          self.pos += 1;
          return Ok(Some(result));
        }
        Bound::Excluded(k) if k.as_slice() > key => {
          self.pos += 1;
          return Ok(Some(result));
        }
        Bound::Unbounded => {
          self.pos += 1;
          return Ok(Some(result));
        }
        _ => {
          self.closed = true;
          return Ok(None);
        }
      }
    }
  }
}
