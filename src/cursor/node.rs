use super::{Key, KeyRef};
use crate::{
  disk::{PageScanner, PageWriter, Pointer, PAGE_SIZE},
  serialize::{Serializable, SerializeType},
  Error, Result,
};

#[derive(Debug)]
pub enum CursorNode {
  Internal(InternalNode),
  Leaf(LeafNode),
}
impl CursorNode {
  pub fn initial_state() -> Self {
    Self::Leaf(LeafNode::new(Default::default(), None))
  }
  pub fn as_leaf(self) -> Result<LeafNode> {
    match self {
      CursorNode::Internal(_) => Err(Error::InvalidFormat("invalid leaf node type")),
      CursorNode::Leaf(node) => Ok(node),
    }
  }
  pub fn as_internal(self) -> Result<InternalNode> {
    match self {
      CursorNode::Internal(node) => Ok(node),
      CursorNode::Leaf(_) => Err(Error::InvalidFormat("invalid internal node type")),
    }
  }
}
impl Serializable for CursorNode {
  fn get_type() -> SerializeType {
    SerializeType::CursorNode
  }
  fn write_at(&self, writer: &mut PageWriter) -> Result {
    match self {
      CursorNode::Internal(node) => {
        writer.write(&[0])?;
        match &node.right {
          Some((pointer, key)) => {
            writer.write(&[1])?;
            writer.write_u64(*pointer)?;
            writer.write_u16(key.len() as u16)?;
            writer.write(key)
          }
          None => writer.write(&[0]),
        }?;
        writer.write_u16(node.keys.len() as u16)?;
        for key in &node.keys {
          writer.write_u16(key.len() as u16)?;
          writer.write(key)?;
        }
        for ptr in &node.children {
          writer.write_u64(*ptr)?;
        }
      }
      CursorNode::Leaf(node) => {
        writer.write(&[1])?;
        writer.write_u64(node.next.unwrap_or(0))?;
        writer.write_u16(node.entries.len() as u16)?;
        for (key, pointer) in &node.entries {
          writer.write_u16(key.len() as u16)?;
          writer.write(&key)?;
          writer.write_u64(*pointer)?;
        }
      }
    }
    Ok(())
  }

  fn read_from(scanner: &mut PageScanner) -> Result<Self> {
    match scanner.read()? {
      0 => {
        //internal
        let mut right = None;
        if scanner.read()? == 1 {
          let ptr = scanner.read_u64()?;
          let len = scanner.read_u16()? as usize;
          let key = scanner.read_n(len)?.to_vec();
          right = Some((ptr, key));
        };

        let len = scanner.read_u16()? as usize;
        let mut keys = Vec::with_capacity(len);
        for _ in 0..len {
          let l = scanner.read_u16()? as usize;
          keys.push(scanner.read_n(l)?.to_vec());
        }

        let mut children = Vec::with_capacity(len + 1);
        for _ in 0..=len {
          children.push(scanner.read_u64()?);
        }
        Ok(Self::Internal(InternalNode::new(keys, children, right)))
      }
      1 => {
        // leaf
        let next = scanner.read_u64()?;
        let len = scanner.read_u16()? as usize;
        let mut entries = Vec::with_capacity(len);
        for _ in 0..len {
          let l = scanner.read_u16()? as usize;
          let key = scanner.read_n(l)?.to_vec();
          let ptr = scanner.read_u64()?;
          entries.push((key, ptr));
        }
        Ok(Self::Leaf(LeafNode::new(
          entries,
          (next != 0).then(|| next),
        )))
      }
      _ => Err(Error::InvalidFormat("invalid cursor node type")),
    }
  }
}

/**
 * B-link tree internal node.
 * right holds the high key and pointer to the right sibling, set when this node
 * is split. If a search key >= high key, traversal must follow the right pointer
 * rather than descending into this node's children.
 */
#[derive(Debug)]
pub struct InternalNode {
  keys: Vec<Key>,
  children: Vec<Pointer>,
  right: Option<(Pointer, Key)>,
}
impl InternalNode {
  pub fn initialize(key: Key, left: Pointer, right: Pointer) -> Self {
    Self::new(vec![key], vec![left, right], None)
  }
  #[inline]
  pub fn to_node(self) -> CursorNode {
    CursorNode::Internal(self)
  }
  fn new(keys: Vec<Key>, children: Vec<Pointer>, right: Option<(Pointer, Key)>) -> Self {
    Self {
      keys,
      children,
      right,
    }
  }
  pub fn find(&self, key: KeyRef) -> std::result::Result<Pointer, Pointer> {
    if let Some((right, high)) = &self.right {
      if high as KeyRef <= key {
        return Err(*right);
      }
    };
    match self.keys.binary_search_by(|k| (k as KeyRef).cmp(key)) {
      Ok(i) => Ok(self.children[i + 1]),
      Err(i) => Ok(self.children[i]),
    }
  }
  pub fn first_child(&self) -> Pointer {
    self.children[0]
  }
  pub fn insert_or_next(
    &mut self,
    key: &Key,
    pointer: Pointer,
  ) -> std::result::Result<(), Pointer> {
    if let Some((right, high)) = &self.right {
      if high <= key {
        return Err(*right);
      }
    };
    let at = self
      .keys
      .binary_search_by(|k| k.cmp(key))
      .unwrap_or_else(|i| i);

    self.keys.insert(at, key.clone());
    self.children.insert(at + 1, pointer);
    Ok(())
  }

  pub fn split_if_needed(&mut self) -> Option<(InternalNode, Key)> {
    let mut byte_len = 1 + 1 + 8 + self.children.len() * 8;
    for i in 0..self.keys.len() {
      byte_len += 8 * 2 + self.keys[i].len();
      if byte_len >= PAGE_SIZE {
        let mid = self.keys.len() >> 1;
        let keys = self.keys.split_off(mid + 1);
        let mid_key = self.keys.pop().unwrap();
        let children = self.children.split_off(mid + 1);

        return Some((
          InternalNode::new(keys, children, self.right.take()),
          mid_key,
        ));
      }
    }
    None
  }

  pub fn set_right(&mut self, key: &Key, ptr: Pointer) -> Option<(Pointer, Key)> {
    self.right.replace((ptr, key.clone()))
  }
  pub fn get_all_child<'a>(&'a self) -> impl Iterator<Item = Pointer> + 'a {
    self.children.iter().map(|i| *i)
  }
}

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
  fn new(entries: Vec<(Key, Pointer)>, next: Option<Pointer>) -> Self {
    Self { entries, next }
  }
  #[inline]
  pub fn to_node(self) -> CursorNode {
    CursorNode::Leaf(self)
  }
  pub fn find(&self, key: KeyRef) -> NodeFindResult {
    match self
      .entries
      .binary_search_by(|(k, _)| (k as KeyRef).cmp(key))
    {
      Ok(i) => NodeFindResult::Found(i, self.entries[i].1),
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
  pub fn at(&self, i: usize) -> &(Key, Pointer) {
    &self.entries[i]
  }
  pub fn len(&self) -> usize {
    self.entries.len()
  }

  pub fn drain(&mut self) -> impl Iterator<Item = (Key, Pointer)> + '_ {
    self.entries.drain(..)
  }
  pub fn get_entry_pointers(&self) -> impl Iterator<Item = Pointer> + '_ {
    self.entries.iter().map(|(_, p)| *p)
  }
  pub fn get_entries(&self) -> impl Iterator<Item = &(Key, Pointer)> + '_ {
    self.entries.iter()
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

  pub fn insert_and_split(
    &mut self,
    index: usize,
    key: Key,
    pointer: Pointer,
  ) -> Option<LeafNode> {
    self.entries.insert(index, (key, pointer));

    let mut byte_len = 1 + 8 + 8 + 8;
    for i in 0..self.entries.len() {
      byte_len += 8 * 2 + self.entries[i].0.len();
      if byte_len >= PAGE_SIZE {
        return Some(LeafNode::new(
          self.entries.split_off(self.entries.len() >> 1),
          self.next.take(),
        ));
      }
    }
    None
  }

  pub fn top(&self) -> &Key {
    &self.entries[0].0
  }
}

#[cfg(test)]
#[path = "tests/node.rs"]
mod tests;
