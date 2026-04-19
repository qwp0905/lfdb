use super::{Key, KeyRef};
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
  keys: Vec<Key>,
  children: Vec<Pointer>,
  right: Option<(Pointer, Key)>,
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
    for _ in 0..len {
      let l = scanner.read_u16()? as usize;
      keys.push(scanner.read_n(l)?.to_vec());
    }

    let mut children = Vec::with_capacity(len + 1);
    for _ in 0..=len {
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
    for key in &self.keys {
      writer.write_u16(key.len() as u16)?;
      writer.write(key)?;
    }
    for ptr in &self.children {
      writer.write_u64(*ptr)?;
    }
    Ok(())
  }
  pub fn initialize(key: Key, left: Pointer, right: Pointer) -> Self {
    Self::new(vec![key], vec![left, right], None)
  }
  pub fn new(
    keys: Vec<Key>,
    children: Vec<Pointer>,
    right: Option<(Pointer, Key)>,
  ) -> Self {
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

  pub fn split_if_needed(&mut self) -> Option<(InternalNode, Key)> {
    let bytes_len = self.bytes_len();
    if bytes_len <= SERIALIZABLE_BYTES {
      return None;
    }

    // node type + right pointer flag + key length + first child pointer
    let mut sum = 1 + 1 + 2 + POINTER_BYTES;
    let mut mid = 0;
    while sum <= bytes_len >> 1 {
      sum += self.keys[mid].len() + 2 + POINTER_BYTES;
      mid += 1;
    }

    let keys = self.keys.split_off(mid + 1);
    let mid_key = self.keys.pop().unwrap();
    let children = self.children.split_off(mid + 1);

    Some((
      InternalNode::new(keys, children, self.right.take()),
      mid_key,
    ))
  }

  pub fn set_right(&mut self, key: &Key, ptr: Pointer) -> Option<(Pointer, Key)> {
    self.right.replace((ptr, key.clone()))
  }
}

pub struct InternalNodeView<'a> {
  page: &'a Page,
  keys: Vec<(usize, usize)>,
  children: Vec<Pointer>,
  right: Option<(Pointer, usize, usize)>,
}
impl<'a> InternalNodeView<'a> {
  pub fn from_scanner(page: &'a Page, scanner: &mut PageScanner) -> Result<Self> {
    let mut right = None;
    if scanner.read()? == 1 {
      let ptr = scanner.read_u64()?;
      let len = scanner.read_u16()? as usize;
      let offset = scanner.offset();
      scanner.read_n(len)?;
      right = Some((ptr, offset, offset + len));
    };

    let len = scanner.read_u16()? as usize;
    let mut keys = Vec::with_capacity(len);
    for _ in 0..len {
      let l = scanner.read_u16()? as usize;
      let offset = scanner.offset();
      scanner.read_n(l)?;
      keys.push((offset, offset + l))
    }

    let mut children = Vec::with_capacity(len + 1);
    for _ in 0..=len {
      children.push(scanner.read_u64()?);
    }
    Ok(Self::new(page, keys, children, right))
  }

  pub fn new(
    page: &'a Page,
    keys: Vec<(usize, usize)>,
    children: Vec<Pointer>,
    right: Option<(Pointer, usize, usize)>,
  ) -> Self {
    Self {
      page,
      keys,
      children,
      right,
    }
  }
  pub fn find(&self, key: KeyRef) -> std::result::Result<Pointer, Pointer> {
    if let Some((right, s, e)) = &self.right {
      if self.page.range(*s..*e) <= key {
        return Err(*right);
      }
    };
    match self
      .keys
      .binary_search_by(|(s, e)| self.page.range(*s..*e).cmp(key))
    {
      Ok(i) => Ok(self.children[i + 1]),
      Err(i) => Ok(self.children[i]),
    }
  }
  pub fn first_child(&self) -> Pointer {
    self.children[0]
  }

  pub fn get_all_child(&self) -> impl Iterator<Item = Pointer> + '_ {
    self.children.iter().map(|i| *i)
  }
}
