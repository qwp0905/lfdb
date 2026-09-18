use std::{
  cell::RefCell,
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
  pub fn find_slot(&self, key: StaticKeyRef) -> FindSlotResult<'_> {
    if let Some((next, high)) = &self.next {
      // B-link right move: the caller may have reached a node whose high key no
      // longer covers this key. In that case the insert belongs to the right sibling.
      if high.as_slice() <= key {
        return FindSlotResult::Move(*next);
      }
    }
    match self.entries.binary_search_by(|r| (*r.key).cmp(key)) {
      Ok(i) => FindSlotResult::Replace(i, &self.entries[i].record, self.entries[i].next),
      Err(i) => FindSlotResult::Insert(i),
    }
  }
  pub fn get_next_key(&self) -> Option<StaticKeyRef<'_>> {
    self.next.as_ref().map(|(_, k)| &**k)
  }
}

pub enum FindSlotResult<'a> {
  Replace(usize, &'a VersionRecord, Option<Pointer>),
  Move(Pointer),
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

#[derive(Debug)]
struct ParsedState {
  offset: usize,
  entries: Option<Vec<LeafEntryView>>,
}
impl ParsedState {
  const fn new(offset: usize) -> Self {
    Self {
      offset,
      entries: None,
    }
  }
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
  len: usize,
  bias: SplitBias,
  next: Option<(Pointer, Range<usize>)>,
  state: RefCell<ParsedState>,
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
      len,
      next,
      bias,
      state: RefCell::new(ParsedState::new(offset)),
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
    if let Some((next, k)) = self.get_next_with_key() {
      if k <= key {
        return Ok(NodeFindResult::Move(next));
      }
    }

    let mut start = 0;
    let mut state = self.state.borrow_mut();
    if let Some(parsed) = state.entries.as_ref() {
      match parsed.binary_search_by(|e| self.page.range(e.range.clone()).cmp(key)) {
        Ok(i) => {
          let e = &parsed[i];
          return Ok(NodeFindResult::Found(i, e.record.clone(), e.next));
        }
        Err(i) if i < parsed.len() => return Ok(NodeFindResult::NotFound(i)),
        Err(i) if parsed.len() == self.len => return Ok(NodeFindResult::NotFound(i)),
        Err(i) => start = i,
      };
    }

    let mut scanner = self.page.scanner();
    scanner.advance(state.offset).unwrap();

    for i in start..self.len {
      let e = LeafEntryView::deserialize_from(&mut scanner)?;
      state.offset = scanner.advance(0).unwrap();

      let parsed = state.entries.get_or_insert_default();
      parsed.push(e);

      let e = &parsed[i];
      let k = self.page.range(e.range.clone());
      if k < key {
        continue;
      } else if k > key {
        return Ok(NodeFindResult::NotFound(i));
      } else {
        return Ok(NodeFindResult::Found(i, e.record.clone(), e.next));
      }
    }
    Ok(NodeFindResult::NotFound(self.len))
  }

  pub fn into_owned(self) -> Result<LeafNode> {
    let next = self
      .next
      .map(|(ptr, range)| (ptr, self.page.copy_range(range)));

    let state = self.state.into_inner();
    let mut scanner = self.page.scanner();
    scanner.advance(state.offset).unwrap();

    let mut entries = Vec::with_capacity(self.len + 1);
    let mut start = 0;
    for e in state.entries.into_iter().flatten() {
      let key = self.page.copy_range(e.range);
      let record = e.record.into_owned_with(self.page);
      let next = e.next;
      entries.push(LeafEntry::new(key, record, next));
      start += 1;
    }

    for _ in start..self.len {
      let l = scanner.read_u16()? as usize;
      let key = scanner.read_n(l)?.to_vec();
      let record = VersionRecord::deserialize_from(&mut scanner)?;
      let next = scanner.read_u64()?;
      entries.push(LeafEntry::new(key, record, (next != 0).then_some(next)))
    }
    Ok(LeafNode::new(entries, next, self.bias))
  }

  pub fn top(&self) -> Result<StaticKeyRef<'_>> {
    let state = self.state.borrow();
    if let Some(parsed) = state.entries.as_ref() {
      return Ok(self.page.range(parsed[0].range.clone()));
    }
    let mut scanner = self.page.scanner();
    scanner.advance(state.offset).unwrap();
    let len = scanner.read_u16()? as usize;
    let offset = scanner.advance(len)?;
    Ok(self.page.range(offset..offset + len))
  }

  pub fn get_entries(&'a self) -> Result<LeafNodeIter<'a>> {
    self.range_entries(&Bound::Unbounded, &Bound::Unbounded)
  }

  pub fn range_entries(
    &'a self,
    start: &'a Bound<StaticKey>,
    end: &'a Bound<StaticKey>,
  ) -> Result<LeafNodeIter<'a>> {
    let (pos, closed) = match start {
      Bound::Included(k) => match self.find(k)? {
        NodeFindResult::Found(i, _, _) => (i, false),
        NodeFindResult::Move(_) => (self.len, true),
        NodeFindResult::NotFound(i) => (i, false),
      },
      Bound::Excluded(k) => match self.find(k)? {
        NodeFindResult::Found(i, _, _) => (i + 1, false),
        NodeFindResult::Move(_) => (self.len, true),
        NodeFindResult::NotFound(i) => (i, false),
      },
      Bound::Unbounded => (0, false),
    };

    Ok(LeafNodeIter {
      node: self,
      end,
      pos,
      closed,
    })
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
  pub fn get_next_key(&self) -> Option<StaticKeyRef<'_>> {
    let Some((_, range)) = &self.next else {
      return None;
    };
    Some(self.page.range(range.clone()))
  }

  fn at(&self, index: usize) -> Result<LeafEntryView> {
    let mut state = self.state.borrow_mut();
    let mut start = 0;
    if let Some(parsed) = state.entries.as_ref() {
      if index < parsed.len() {
        return Ok(parsed[index].clone());
      }
      start = parsed.len();
    };

    let mut scanner = self.page.scanner();
    scanner.advance(state.offset).unwrap();
    for _ in start..=index {
      let e = LeafEntryView::deserialize_from(&mut scanner)?;
      state.offset = scanner.advance(0).unwrap();
      let parsed = state.entries.get_or_insert_default();
      parsed.push(e);
    }

    Ok(state.entries.as_ref().unwrap()[index].clone())
  }
}

#[derive(Debug)]
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
impl Clone for LeafEntryView {
  fn clone(&self) -> Self {
    Self {
      range: self.range.clone(),
      record: self.record.clone(),
      next: self.next,
    }
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
  node: &'a LeafNodeView<'a>,
  end: &'a Bound<StaticKey>,
  pos: usize,
  /**
   * Set after the iterator reaches the end or passes the upper bound.
   */
  closed: bool,
}
impl<'a> LeafNodeIter<'a> {
  pub const fn is_completed(&self) -> bool {
    self.pos == self.node.len
  }

  /**
   * Return the next entry within the configured key bounds.
   *
   * The returned `(start, end)` is the key byte range in `page`.
   */
  pub fn try_next(&mut self) -> Result<Option<LeafEntryView>> {
    if self.closed {
      return Ok(None);
    }
    if self.is_completed() {
      self.closed = true;
      return Ok(None);
    }

    let e = self.node.at(self.pos)?;
    let key = self.node.page.range(e.range.clone());

    match self.end {
      Bound::Included(k) if k.as_slice() >= key => {
        self.pos += 1;
        Ok(Some(e))
      }
      Bound::Excluded(k) if k.as_slice() > key => {
        self.pos += 1;
        Ok(Some(e))
      }
      Bound::Unbounded => {
        self.pos += 1;
        Ok(Some(e))
      }
      _ => {
        self.closed = true;
        Ok(None)
      }
    }
  }
}
