use super::{StaticKey, StaticKeyRef};
use crate::{
  disk::{Page, PageScanner, PageWriter, Pointer, POINTER_BYTES},
  serialize::SERIALIZABLE_BYTES,
  Result,
};

/**
 * B-link tree internal node.
 * right holds the high key and pointer to the right sibling, set when this node
 * is split. If a search key >= high key, traversal must follow the right pointer
 * rather than descending into this node's children.
 */
#[derive(Debug)]
pub struct InternalNode {
  keys: Vec<StaticKey>,
  children: Vec<Pointer>,
  right: Option<(Pointer, StaticKey)>,
}
impl InternalNode {
  pub fn from_scanner(scanner: &mut PageScanner) -> Result<Self> {
    let mut right = None;
    if scanner.read()? == 1 {
      let ptr = scanner.read_u64()?;
      let len = scanner.read_u16()? as usize;
      let key = scanner.read_n(len)?.to_vec();
      right = Some((ptr, key));
    };

    let len = scanner.read_u16()? as usize;
    let mut keys = Vec::with_capacity(len);
    let mut children = Vec::with_capacity(len + 1);
    children.push(scanner.read_u64()?);
    for _ in 0..len {
      let l = scanner.read_u16()? as usize;
      keys.push(scanner.read_n(l)?.to_vec());
      children.push(scanner.read_u64()?);
    }

    Ok(Self::new(keys, children, right))
  }
  pub fn write_at(&self, writer: &mut PageWriter) -> Result {
    match &self.right {
      Some((pointer, key)) => {
        writer.write(&[1])?;
        writer.write_u64(*pointer)?;
        writer.write_u16(key.len() as u16)?;
        writer.write(key)
      }
      None => writer.write(&[0]),
    }?;
    writer.write_u16(self.keys.len() as u16)?;
    writer.write_u64(self.children[0])?;
    for i in 0..self.keys.len() {
      let key = &self.keys[i];
      let ptr = self.children[i + 1];
      writer.write_u16(key.len() as u16)?;
      writer.write(key)?;
      writer.write_u64(ptr)?;
    }
    Ok(())
  }
  pub fn initialize(key: StaticKey, left: Pointer, right: Pointer) -> Self {
    Self::new(vec![key], vec![left, right], None)
  }
  pub const fn new(
    keys: Vec<StaticKey>,
    children: Vec<Pointer>,
    right: Option<(Pointer, StaticKey)>,
  ) -> Self {
    Self {
      keys,
      children,
      right,
    }
  }

  pub fn insert_or_next(
    &mut self,
    key: &StaticKey,
    pointer: Pointer,
  ) -> std::result::Result<usize, Pointer> {
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
    Ok(at)
  }

  #[inline]
  fn bytes_len(&self) -> usize {
    1 + 1
      + self
        .right
        .as_ref()
        .map(|(_, k)| k.len() + POINTER_BYTES + 2)
        .unwrap_or(0)
      + 2
      + self.children.len() * POINTER_BYTES
      + self.keys.iter().map(|k| k.len()).sum::<usize>()
      + self.keys.len() * 2
  }

  pub fn split_if_needed(
    &mut self,
    insert_at: usize,
  ) -> Option<(InternalNode, StaticKey)> {
    let bytes_len = self.bytes_len();
    if bytes_len <= SERIALIZABLE_BYTES {
      return None;
    }

    debug_assert!(self.keys.len() > 2);
    let len = self.keys.len();
    let split_point = match insert_at {
      i if i < len / 3 => bytes_len >> 2,
      i if i < (len << 1) / 3 => bytes_len >> 1,
      _ => (bytes_len >> 2) * 3,
    };

    // node type + right pointer flag + key length + first child pointer
    let mut sum = 1 + 1 + 2 + POINTER_BYTES;
    let mut mid = 0;
    while mid == 0 || (sum < split_point && mid < len - 2) {
      sum += self.keys[mid].len() + 2 + POINTER_BYTES;
      mid += 1;
    }

    let keys = self.keys.split_off(mid + 1);
    let mid_key = self.keys.pop().unwrap();
    let children = self.children.split_off(mid + 1);

    debug_assert!(!self.keys.is_empty());
    debug_assert!(!keys.is_empty());

    Some((
      InternalNode::new(keys, children, self.right.take()),
      mid_key,
    ))
  }

  pub fn set_right(
    &mut self,
    key: &StaticKey,
    ptr: Pointer,
  ) -> Option<(Pointer, StaticKey)> {
    self.right.replace((ptr, key.clone()))
  }
}

pub struct InternalNodeView<'a> {
  page: &'a Page,
  len: usize,
  offset: usize,
  right: Option<(Pointer, usize, usize)>,
}
impl<'a> InternalNodeView<'a> {
  pub fn from_scanner(page: &'a Page, scanner: &mut PageScanner) -> Result<Self> {
    let mut right = None;
    if scanner.read()? == 1 {
      let ptr = scanner.read_u64()?;
      let len = scanner.read_u16()? as usize;
      let offset = scanner.advance(len)?;
      right = Some((ptr, offset, offset + len));
    };

    let len = scanner.read_u16()? as usize;
    let offset = scanner.advance(0)?;

    Ok(Self::new(page, len, offset, right))
  }

  pub const fn new(
    page: &'a Page,
    len: usize,
    offset: usize,
    right: Option<(Pointer, usize, usize)>,
  ) -> Self {
    Self {
      page,
      len,
      offset,
      right,
    }
  }
  pub fn find(&self, key: StaticKeyRef) -> Result<std::result::Result<Pointer, Pointer>> {
    if let Some((right, s, e)) = &self.right {
      if self.page.range(*s..*e) <= key {
        return Ok(Err(*right));
      };
    }

    let mut scanner = self.page.scanner();
    let _ = scanner.advance(self.offset);

    let mut prev_child = scanner.read_u64()?;
    for _ in 0..self.len {
      let l = scanner.read_u16()? as usize;
      let offset = scanner.advance(l)?;
      let k = self.page.range(offset..(offset + l));
      let child = scanner.read_u64()?;
      if k < key {
        prev_child = child;
        continue;
      } else if k == key {
        return Ok(Ok(child));
      } else {
        return Ok(Ok(prev_child));
      }
    }

    Ok(Ok(prev_child))
  }
  pub fn first_child(&self) -> Result<Pointer> {
    let mut scanner = self.page.scanner();
    scanner.advance(self.offset).unwrap();
    scanner.read_u64()
  }

  pub fn get_all_child(&self) -> Result<Vec<Pointer>> {
    let mut scanner = self.page.scanner();
    scanner.advance(self.offset).unwrap();

    let mut children = Vec::with_capacity(self.len + 1);
    children.push(scanner.read_u64()?);

    for _ in 0..self.len {
      let l = scanner.read_u16()? as usize;
      scanner.advance(l)?;
      children.push(scanner.read_u64()?);
    }

    Ok(children)
  }
}
