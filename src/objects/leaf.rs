use std::{
  mem::replace,
  ops::{Bound, Range},
};

use super::{
  count_directions, update_bias, SplitBias, StaticKey, StaticKeyRef, VersionRecord,
  VersionRecordView, DEFAULT_BIAS, MAX_KEY, SERIALIZABLE_BYTES, SPLIT_BIAS_BYTES,
};
use crate::{
  disk::{Page, PageScanner, PageWriter, Pointer, POINTER_BYTES},
  Result,
};

/**
 * Entry stored inside a leaf node.
 *
 * The leaf stores the key and the latest version record inline. `next` points
 * to the data-entry page that continues the version chain for older records.
 */
#[derive(Debug)]
pub struct LeafEntry {
  pub key: StaticKey,
  pub record: VersionRecord,
  pub next: Option<Pointer>,
}
impl LeafEntry {
  const fn new(key: StaticKey, record: VersionRecord, next: Option<Pointer>) -> Self {
    Self { key, record, next }
  }

  const fn bytes_len(&self) -> usize {
    self.key.len() + Self::RESERVED_BYTES + self.record.byte_len()
  }
  const RESERVED_BYTES: usize = POINTER_BYTES + 2;
}

/**
 * Maximum inline value size that still lets a leaf node hold at least two inline
 * value entries. Larger values must be stored as blobs instead of occupying leaf
 * payload directly.
 */
pub const LARGE_VALUE: usize =
  ((SERIALIZABLE_BYTES - (LeafNode::RESERVED_BYTES + MAX_KEY + POINTER_BYTES + 2)) >> 1)
    - (MAX_KEY + LeafEntry::RESERVED_BYTES + VersionRecord::RESERVED_BYTES + 1 + 2);

/**
 * B-link tree leaf node.
 *
 * `LeafNode::next` links this leaf to the right sibling in key order. It is
 * separate from `LeafEntry::next`, which links a single key to its version
 * chain.
 */
#[derive(Debug)]
pub struct LeafNode {
  entries: Vec<LeafEntry>,
  next: Option<(Pointer, StaticKey)>,
  bias: SplitBias,
}
impl LeafNode {
  pub const fn empty() -> Self {
    Self::new(Vec::new(), None, DEFAULT_BIAS)
  }
  const fn new(
    entries: Vec<LeafEntry>,
    next: Option<(Pointer, StaticKey)>,
    bias: SplitBias,
  ) -> Self {
    Self {
      entries,
      next,
      bias,
    }
  }

  pub fn entries_mut(&mut self) -> impl Iterator<Item = &mut LeafEntry> {
    self.entries.iter_mut()
  }

  pub fn write_at(&self, writer: &mut PageWriter) -> Result {
    match &self.next {
      Some((ptr, key)) => {
        writer.write(&[1])?;
        writer.write_u64(*ptr)?;
        writer.write_u16(key.len() as u16)?;
        writer.write(key)?;
      }
      None => writer.write(&[0])?,
    };
    writer.write_u16(self.entries.len() as u16)?;
    writer.write_u32(self.bias)?;
    for entry in &self.entries {
      writer.write_u16(entry.key.len() as u16)?;
      writer.write(&entry.key)?;
      entry.record.serialize_to(writer)?;
      writer.write_u64(entry.next.unwrap_or(0))?;
    }
    Ok(())
  }

  pub fn from_scanner(scanner: &mut PageScanner) -> Result<Self> {
    let mut next = None;
    if scanner.read()? == 1 {
      let ptr = scanner.read_u64()?;
      let len = scanner.read_u16()? as usize;
      let key = scanner.read_n(len)?.to_vec();
      next = Some((ptr, key));
    };
    let len = scanner.read_u16()? as usize;
    let bias = scanner.read_u32()?;
    let mut entries = Vec::with_capacity(len);
    for _ in 0..len {
      let l = scanner.read_u16()? as usize;
      let key = scanner.read_n(l)?.to_vec();
      let record = VersionRecord::deserialize_from(scanner)?;
      let next = scanner.read_u64()?;
      entries.push(LeafEntry::new(key, record, (next != 0).then_some(next)))
    }
    Ok(Self::new(entries, next, bias))
  }

  pub const fn set_next(
    &mut self,
    key: StaticKey,
    pointer: Pointer,
  ) -> Option<(Pointer, StaticKey)> {
    self.next.replace((pointer, key))
  }
  pub const fn get_next(&self) -> Option<Pointer> {
    match &self.next {
      Some((p, _)) => Some(*p),
      None => None,
    }
  }

  #[inline]
  fn data_bytes(&self) -> usize {
    self.entries.iter().map(|e| e.bytes_len()).sum::<usize>()
  }
  // node type + right pointer flag + entry len (u16) + bias
  const RESERVED_BYTES: usize = 1 + 1 + 2 + SPLIT_BIAS_BYTES;
  const fn right_bytes(&self) -> usize {
    let Some((_, k)) = &self.next else {
      return 0;
    };
    k.len() + 2 + POINTER_BYTES
  }

  pub fn split_if_needed(&mut self) -> Option<LeafNode> {
    let right_bytes = self.right_bytes();
    let data_bytes = self.data_bytes();
    if right_bytes + data_bytes + Self::RESERVED_BYTES <= SERIALIZABLE_BYTES {
      return None;
    }
    let (multiplier, divisor) = {
      let [l, m, r] = count_directions(replace(&mut self.bias, DEFAULT_BIAS));
      debug_assert!((l + m + r) != 0);
      ((m + (r << 1)), ((l + r + m) << 1))
    };

    debug_assert!(self.entries.len() > 1);

    let mut best = None;
    let mut left_data = 0;

    for mid in 1..self.entries.len() {
      left_data += self.entries[mid - 1].bytes_len();

      let split_key_bytes = self.entries[mid].key.len() + 2 + POINTER_BYTES;
      let right_data = data_bytes - left_data;

      let left_total = Self::RESERVED_BYTES + left_data + split_key_bytes;
      let right_total = Self::RESERVED_BYTES + right_bytes + right_data;

      if left_total > SERIALIZABLE_BYTES {
        break;
      }
      if right_total > SERIALIZABLE_BYTES {
        continue;
      }

      let split_point =
        (left_data + split_key_bytes + right_bytes + right_data) * multiplier / divisor;
      let dist = (left_data + split_key_bytes).abs_diff(split_point);
      if best.is_none_or(|(_, best_dist)| dist < best_dist) {
        best = Some((mid, dist));
      }
    }

    let entries = self.entries.split_off(best.map(|(mid, _)| mid).unwrap());
    let split = Self::new(entries, self.next.take(), DEFAULT_BIAS);

    debug_assert!(!self.entries.is_empty());
    debug_assert!(!split.entries.is_empty());
    Some(split)
  }

  pub fn replace_at(&mut self, pos: usize, record: VersionRecord) -> VersionRecord {
    replace(&mut self.entries[pos].record, record)
  }
  pub fn insert_at(&mut self, pos: usize, key: StaticKey, record: VersionRecord) {
    self.entries.insert(pos, LeafEntry::new(key, record, None));
    self.bias = update_bias(self.bias, self.entries.len(), pos);
  }
  pub fn alloc_entry_at(&mut self, pos: usize, entry_ptr: Pointer) {
    self.entries[pos].next = Some(entry_ptr);
  }

  pub fn top(&self) -> &StaticKey {
    &self.entries[0].key
  }
  pub fn find_slot(&self, key: StaticKeyRef) -> FindSlotResult {
    if let Some((next, high)) = &self.next {
      // B-link right move: the caller may have reached a node whose high key no
      // longer covers this key. In that case the insert belongs to the right sibling.
      if high.as_slice() <= key {
        return FindSlotResult::Move(*next);
      }
    }
    match self.entries.binary_search_by(|r| (*r.key).cmp(key)) {
      Ok(i) => FindSlotResult::Replace(i),
      Err(i) => FindSlotResult::Insert(i),
    }
  }
}

pub enum FindSlotResult {
  Replace(usize),
  Move(Pointer),
  #[allow(unused)]
  Insert(usize),
}

/**
 * Result of a leaf node key lookup.
 * Move is the B-link tree right-move: the key falls beyond this node's range,
 * so the caller must follow the next pointer to the right sibling — the same
 * mechanism used at the internal level when a search key >= high key.
 */
pub enum NodeFindResult {
  Found(usize, VersionRecordView, Option<Pointer>),
  Move(Pointer),
  NotFound(usize),
}

/**
 * Zero-copy view of a serialized leaf node.
 *
 * Like `InternalNodeView`, this is the read-only traversal form. It borrows the
 * page and reads keys/records by offset. Mutation paths materialize an owned
 * `LeafNode` through `into_owned`.
 */
#[derive(Debug)]
pub struct LeafNodeView<'a> {
  page: &'a Page,
  offset: usize,
  len: usize,
  bias: SplitBias,
  next: Option<(Pointer, Range<usize>)>,
}
impl<'a> LeafNodeView<'a> {
  const fn new(
    page: &'a Page,
    offset: usize,
    len: usize,
    next: Option<(Pointer, Range<usize>)>,
    bias: SplitBias,
  ) -> Self {
    Self {
      page,
      offset,
      len,
      next,
      bias,
    }
  }
  pub fn from_scanner(page: &'a Page, scanner: &mut PageScanner<'a>) -> Result<Self> {
    let mut next = None;
    if scanner.read()? == 1 {
      let ptr = scanner.read_u64()?;
      let len = scanner.read_u16()? as usize;
      let offset = scanner.advance(len)?;
      next = Some((ptr, offset..(offset + len)));
    };
    let len = scanner.read_u16()? as usize;
    let bias = scanner.read_u32()?;
    let offset = scanner.advance(0)?;
    Ok(Self::new(page, offset, len, next, bias))
  }

  pub fn find(&self, key: StaticKeyRef) -> Result<NodeFindResult> {
    if let Some((next, range)) = &self.next {
      if self.page.range(range.clone()) <= key {
        return Ok(NodeFindResult::Move(*next));
      }
    }

    let mut scanner = self.page.scanner();
    scanner.advance(self.offset).unwrap();

    for i in 0..self.len {
      let e = LeafEntryView::deserialize_from(&mut scanner)?;
      let k = self.page.range(e.range);
      if k < key {
        continue;
      } else if k > key {
        return Ok(NodeFindResult::NotFound(i));
      } else {
        return Ok(NodeFindResult::Found(i, e.record, e.next));
      }
    }
    Ok(NodeFindResult::NotFound(self.len))
  }

  pub fn into_owned(self) -> Result<LeafNode> {
    let next = self
      .next
      .map(|(ptr, range)| (ptr, self.page.copy_range(range)));

    let mut scanner = self.page.scanner();
    scanner.advance(self.offset).unwrap();

    let mut entries = Vec::with_capacity(self.len + 1);
    for _ in 0..self.len {
      let l = scanner.read_u16()? as usize;
      let key = scanner.read_n(l)?.to_vec();
      let record = VersionRecord::deserialize_from(&mut scanner)?;
      let next = scanner.read_u64()?;
      entries.push(LeafEntry::new(key, record, (next != 0).then_some(next)))
    }
    Ok(LeafNode::new(entries, next, self.bias))
  }

  pub fn top(&self) -> Result<StaticKeyRef<'_>> {
    let mut scanner = self.page.scanner();
    scanner.advance(self.offset).unwrap();
    let len = scanner.read_u16()? as usize;
    let offset = scanner.advance(len)?;
    Ok(self.page.range(offset..offset + len))
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
    let Some((p, _)) = &self.next else {
      return None;
    };
    Some(*p)
  }
  pub fn get_next_with_key(&self) -> Option<(Pointer, StaticKeyRef<'_>)> {
    let Some((p, range)) = &self.next else {
      return None;
    };
    Some((*p, self.page.range(range.clone())))
  }
}

pub struct LeafEntryView {
  pub range: Range<usize>,
  pub record: VersionRecordView,
  pub next: Option<Pointer>,
}
impl LeafEntryView {
  fn deserialize_from(scanner: &mut PageScanner) -> Result<Self> {
    let l = scanner.read_u16()? as usize;
    let offset = scanner.advance(l)?;
    let record = VersionRecordView::deserialize_from(scanner)?;
    let ptr = scanner.read_u64()?;
    Ok(Self {
      range: offset..(offset + l),
      record,
      next: (ptr != 0).then_some(ptr),
    })
  }
}

/**
 * Sequential iterator over entries in a serialized leaf node.
 *
 * The iterator walks the page bytes directly. For each entry it returns the key
 * byte range inside the page, the inline version-record view, and the pointer to
 * the rest of that key's version chain. It does not allocate or decide whether
 * the caller should copy or borrow the key bytes.
 */
pub struct LeafNodeIter<'a> {
  scanner: PageScanner<'a>,
  page: &'a Page,
  start: &'a Bound<StaticKey>,
  end: &'a Bound<StaticKey>,
  pos: usize,
  len: usize,
  /**
   * Set after the iterator reaches the end or passes the upper bound.
   */
  closed: bool,
}
impl<'a> LeafNodeIter<'a> {
  pub const fn is_completed(&self) -> bool {
    self.pos == self.len
  }

  /**
   * Return the next entry within the configured key bounds.
   *
   * The returned `(start, end)` is the key byte range in `page`.
   */
  pub fn try_next(&mut self) -> Result<Option<LeafEntryView>> {
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

      let e = LeafEntryView {
        range: offset..(offset + l),
        record,
        next: (ptr != 0).then_some(ptr),
      };

      match self.end {
        Bound::Included(k) if k.as_slice() >= key => {
          self.pos += 1;
          return Ok(Some(e));
        }
        Bound::Excluded(k) if k.as_slice() > key => {
          self.pos += 1;
          return Ok(Some(e));
        }
        Bound::Unbounded => {
          self.pos += 1;
          return Ok(Some(e));
        }
        _ => {
          self.closed = true;
          return Ok(None);
        }
      }
    }
  }
}
