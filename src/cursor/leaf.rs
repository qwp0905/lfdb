use super::{Key, KeyRef};
use crate::{
  disk::{Page, PageScanner, PageWriter, Pointer, POINTER_BYTES},
  serialize::SERIALIZABLE_BYTES,
  Result,
};

/**
 * Result of a leaf node key lookup.
 * Move is the B-link tree right-move: the key falls beyond this node's range,
 * so the caller must follow the next pointer to the right sibling — the same
 * mechanism used at the internal level when a search key >= high key.
 */
pub enum NodeFindResult {
  Found(usize, Pointer),
  Move(Pointer),
  NotFound(usize),
}
/**
 * B+tree leaf node. Leaf nodes are linked in key order via next, set when this
 * node is split to chain the new right sibling into the list.
 */
#[derive(Debug)]
pub struct LeafNode {
  entries: Vec<(Key, Pointer)>,
  next: Option<Pointer>,
}
impl LeafNode {
  pub fn new(entries: Vec<(Key, Pointer)>, next: Option<Pointer>) -> Self {
    Self { entries, next }
  }

  pub fn write_at(&self, writer: &mut PageWriter) -> Result {
    writer.write_u64(self.next.unwrap_or(0))?;
    writer.write_u16(self.entries.len() as u16)?;
    for (key, pointer) in &self.entries {
      writer.write_u16(key.len() as u16)?;
      writer.write(&key)?;
      writer.write_u64(*pointer)?;
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
      let ptr = scanner.read_u64()?;
      entries.push((key, ptr));
    }
    Ok(Self::new(entries, (next != 0).then(|| next)))
  }

  pub fn len(&self) -> usize {
    self.entries.len()
  }

  pub fn drain(&mut self) -> impl Iterator<Item = (Key, Pointer)> + '_ {
    self.entries.drain(..)
  }

  pub fn set_entries(&mut self, entries: Vec<(Key, Pointer)>) {
    self.entries = entries;
  }

  pub fn get_next(&self) -> Option<Pointer> {
    self.next
  }
  pub fn set_next(&mut self, pointer: Pointer) -> Option<Pointer> {
    self.next.replace(pointer)
  }

  #[inline]
  fn bytes_len(&self) -> usize {
    1 + POINTER_BYTES
      + 2
      + self.entries.iter().map(|(k, _)| k.len()).sum::<usize>()
      + self.entries.len() * (POINTER_BYTES + 2)
  }

  pub fn insert_and_split(
    &mut self,
    index: usize,
    key: Key,
    pointer: Pointer,
  ) -> Option<LeafNode> {
    self.entries.insert(index, (key, pointer));

    let bytes_len = self.bytes_len();
    if bytes_len <= SERIALIZABLE_BYTES {
      return None;
    }

    let mut sum = 1 + POINTER_BYTES + 2;
    let mut mid = 0;
    while sum <= bytes_len >> 1 {
      sum += self.entries[mid].0.len() + 2 + POINTER_BYTES;
      mid += 1;
    }

    Some(Self::new(self.entries.split_off(mid), self.next.take()))
  }

  pub fn top(&self) -> &Key {
    &self.entries[0].0
  }
}

#[derive(Debug)]
pub struct LeafNodeView<'a> {
  page: &'a Page,
  entries: Vec<(usize, usize, Pointer)>,
  next: Option<Pointer>,
}
impl<'a> LeafNodeView<'a> {
  fn new(
    page: &'a Page,
    entries: Vec<(usize, usize, Pointer)>,
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
      let offset = scanner.offset();
      scanner.read_n(l)?;
      let ptr = scanner.read_u64()?;
      entries.push((offset, offset + l, ptr));
    }
    Ok(Self::new(page, entries, (next != 0).then(|| next)))
  }

  pub fn writable(self) -> LeafNode {
    LeafNode {
      entries: self.get_entries().map(|(k, p)| (k.to_vec(), p)).collect(),
      next: self.next,
    }
  }

  pub fn find(&self, key: KeyRef) -> NodeFindResult {
    match self
      .entries
      .binary_search_by(|(s, e, _)| (self.page.range(*s..*e)).cmp(key))
    {
      Ok(i) => NodeFindResult::Found(i, self.entries[i].2),
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

  pub fn get_entries(&self) -> impl Iterator<Item = (KeyRef<'a>, Pointer)> + '_ {
    self
      .entries
      .iter()
      .map(|(s, e, p)| (self.page.range(*s..*e), *p))
  }

  pub fn get_entry_pointers(&self) -> impl Iterator<Item = Pointer> + '_ {
    self.entries.iter().map(|(_, _, p)| *p)
  }

  pub fn get_next(&self) -> Option<Pointer> {
    self.next
  }
}
