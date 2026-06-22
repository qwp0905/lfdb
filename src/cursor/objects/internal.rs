use std::mem::replace;

use super::{
  count_directions, update_bias, SplitBias, StaticKey, StaticKeyRef, DEFAULT_BIAS,
  SPLIT_BIAS_BYTES,
};
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
  bias: SplitBias,
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
    let bias = scanner.read_u32()?;
    let mut keys = Vec::with_capacity(len);
    let mut children = Vec::with_capacity(len + 1);
    children.push(scanner.read_u64()?);
    for _ in 0..len {
      let l = scanner.read_u16()? as usize;
      keys.push(scanner.read_n(l)?.to_vec());
      children.push(scanner.read_u64()?);
    }

    Ok(Self::new(keys, children, right, bias))
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
    writer.write_u32(self.bias)?;
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
    Self::new(vec![key], vec![left, right], None, DEFAULT_BIAS)
  }
  pub const fn new(
    keys: Vec<StaticKey>,
    children: Vec<Pointer>,
    right: Option<(Pointer, StaticKey)>,
    bias: SplitBias,
  ) -> Self {
    Self {
      keys,
      children,
      right,
      bias,
    }
  }

  pub fn insert_or_next(
    &mut self,
    key: &StaticKey,
    pointer: Pointer,
  ) -> std::result::Result<(), Pointer> {
    if let Some((right, high)) = &self.right {
      if high <= key {
        return Err(*right);
      }
    };
    let pos = self
      .keys
      .binary_search_by(|k| k.cmp(key))
      .unwrap_or_else(|i| i);

    self.keys.insert(pos, key.clone());
    self.children.insert(pos + 1, pointer);
    self.bias = update_bias(self.bias, self.keys.len(), pos);
    Ok(())
  }

  // node type + right pointer flag + key len (u16) + bias
  const BASE_BYTES: usize = 1 + 1 + 2 + SPLIT_BIAS_BYTES;
  #[inline]
  const fn key_bytes(key: &StaticKey) -> usize {
    key.len() + 2 + POINTER_BYTES
  }
  fn right_bytes(&self) -> usize {
    self
      .right
      .as_ref()
      .map(|(_, k)| Self::key_bytes(k))
      .unwrap_or(0)
  }
  fn keys_bytes(&self) -> usize {
    self.keys.iter().map(Self::key_bytes).sum::<usize>() + POINTER_BYTES
  }

  #[inline]
  fn bytes_len(&self) -> usize {
    Self::BASE_BYTES + self.keys_bytes() + self.right_bytes()
  }

  pub fn split_if_needed(&mut self) -> Option<(InternalNode, StaticKey)> {
    let right_bytes = self.right_bytes();
    let keys_bytes = self.keys_bytes();
    let split_bytes = right_bytes + keys_bytes;
    if split_bytes + Self::BASE_BYTES <= SERIALIZABLE_BYTES {
      return None;
    }

    let split_point = {
      let [l, m, r] = count_directions(replace(&mut self.bias, DEFAULT_BIAS));
      debug_assert!((l + m + r) != 0);
      split_bytes * (m + (r << 1)) / ((l + m + r) << 1)
    };

    debug_assert!(self.keys.len() > 2);

    let mut best = None;
    let mut left_bytes = 0;

    for mid in 1..(self.keys.len() - 1) {
      left_bytes += Self::key_bytes(&self.keys[mid - 1]);

      let split_key_bytes = Self::key_bytes(&self.keys[mid]);
      let right_key_bytes = keys_bytes - left_bytes - split_key_bytes;
      let left_total = Self::BASE_BYTES + left_bytes + split_key_bytes;
      let right_total = Self::BASE_BYTES + right_bytes + right_key_bytes;

      if left_total > SERIALIZABLE_BYTES {
        continue;
      }
      if right_total > SERIALIZABLE_BYTES {
        continue;
      }

      let dist = (left_bytes + split_key_bytes).abs_diff(split_point);
      if best.is_none_or(|(_, best_dist)| dist < best_dist) {
        best = Some((mid, dist));
      }
    }

    let mid = best.unwrap().0;
    let keys = self.keys.split_off(mid + 1);
    let mid_key = self.keys.pop().unwrap();
    let children = self.children.split_off(mid + 1);
    let split = InternalNode::new(keys, children, self.right.take(), Default::default());

    debug_assert!(split.bytes_len() <= SERIALIZABLE_BYTES);
    debug_assert!(!split.keys.is_empty());
    debug_assert!(!self.keys.is_empty());
    debug_assert!(self.bytes_len() <= SERIALIZABLE_BYTES);

    Some((split, mid_key))
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
    scanner.advance(SPLIT_BIAS_BYTES)?;
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
