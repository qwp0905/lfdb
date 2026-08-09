use std::{mem::replace, ops::Range};

use super::{
  count_directions, update_bias, SplitBias, StaticKey, StaticKeyRef, DEFAULT_BIAS,
  SERIALIZABLE_BYTES, SPLIT_BIAS_BYTES,
};
use crate::{
  disk::{Page, PageScanner, PageWriter, Pointer, POINTER_BYTES},
  Result,
};

/**
 * B-link tree internal node.
 *
 * `right` is the B-link right-move link created by split. It stores the right
 * sibling pointer and this node's high key. If a search or insert key is greater
 * than or equal to the high key, traversal must move right instead of descending
 * through this node's children.
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
      // B-link right move: the caller may have reached a node whose high key no
      // longer covers this key. In that case the insert belongs to the right sibling.
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
  const RESERVED_BYTES: usize = 1 + 1 + 2 + SPLIT_BIAS_BYTES;
  #[inline]
  const fn key_bytes(key: &StaticKey) -> usize {
    key.len() + 2 + POINTER_BYTES
  }
  const fn right_bytes(&self) -> usize {
    let Some((_, k)) = &self.right else {
      return 0;
    };
    Self::key_bytes(k)
  }
  fn keys_bytes(&self) -> usize {
    self.keys.iter().map(Self::key_bytes).sum::<usize>() + POINTER_BYTES
  }

  /**
   * Split this internal node if it no longer fits.
   *
   * Returns the new right node and the separator key. The separator is also the
   * left node's B-link high key: the caller must install it with `set_right` after
   * allocating the right sibling, and also propagate it to the parent.
   */
  pub fn split_if_needed(&mut self) -> Option<(InternalNode, StaticKey)> {
    let right_bytes = self.right_bytes();
    let keys_bytes = self.keys_bytes();
    let split_bytes = right_bytes + keys_bytes;
    if split_bytes + Self::RESERVED_BYTES <= SERIALIZABLE_BYTES {
      return None;
    }

    let split_point = {
      // Consume the insertion-position bias as a direction histogram. The histogram
      // chooses the target split position: left-heavy history moves the split target
      // left, right-heavy history moves it right, and middle-heavy history keeps it
      // near the center.
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

      // The middle key is removed from the child-key list, but it becomes this
      // node's high key through `set_right` after the split. Count it on the left side
      // when checking whether both serialized nodes will fit.
      let left_total = Self::RESERVED_BYTES + left_bytes + split_key_bytes;
      let right_total = Self::RESERVED_BYTES + right_bytes + right_key_bytes;

      if left_total > SERIALIZABLE_BYTES {
        break;
      }
      if right_total > SERIALIZABLE_BYTES {
        continue;
      }

      let dist = (left_bytes + split_key_bytes).abs_diff(split_point);
      if best.is_none_or(|(_, best_dist)| dist < best_dist) {
        best = Some((mid, dist));
      }
    }

    let mid = best.map(|(mid, _)| mid).unwrap();
    let keys = self.keys.split_off(mid + 1);
    let mid_key = self.keys.pop().unwrap();
    let children = self.children.split_off(mid + 1);
    let split = InternalNode::new(keys, children, self.right.take(), DEFAULT_BIAS);

    debug_assert!(!split.keys.is_empty());
    debug_assert!(!self.keys.is_empty());
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

/**
 * Zero-copy view of a serialized internal node.
 *
 * The owned `InternalNode` is used when the node must be modified and written
 * back. `InternalNodeView` is the read-only traversal form: it stores offsets
 * into the page and compares keys directly against page byte ranges.
 */
pub struct InternalNodeView<'a> {
  page: &'a Page,
  len: usize,
  offset: usize,
  right: Option<(Pointer, Range<usize>)>,
}
impl<'a> InternalNodeView<'a> {
  pub fn from_scanner(page: &'a Page, scanner: &mut PageScanner) -> Result<Self> {
    let mut right = None;
    if scanner.read()? == 1 {
      let ptr = scanner.read_u64()?;
      let len = scanner.read_u16()? as usize;
      let offset = scanner.advance(len)?;
      right = Some((ptr, offset..(offset + len)));
    };

    let len = scanner.read_u16()? as usize;
    scanner.advance(SPLIT_BIAS_BYTES)?;
    let offset = scanner.advance(0)?;

    Ok(Self::new(page, len, offset, right))
  }

  const fn new(
    page: &'a Page,
    len: usize,
    offset: usize,
    right: Option<(Pointer, Range<usize>)>,
  ) -> Self {
    Self {
      page,
      len,
      offset,
      right,
    }
  }
  pub fn find_excluded(
    &self,
    key: StaticKeyRef,
  ) -> Result<std::result::Result<(usize, Pointer), Pointer>> {
    if let Some((right, range)) = &self.right {
      if self.page.range(range.clone()) < key {
        return Ok(Err(*right));
      };
    }

    let mut scanner = self.page.scanner();
    scanner.advance(self.offset).unwrap();

    let mut prev_child = scanner.read_u64()?;
    for i in 0..self.len {
      let l = scanner.read_u16()? as usize;
      let offset = scanner.advance(l)?;
      let k = self.page.range(offset..(offset + l));
      let child = scanner.read_u64()?;
      if k < key {
        prev_child = child;
        continue;
      } else {
        return Ok(Ok((i, prev_child)));
      }
    }
    Ok(Ok((self.len, prev_child)))
  }

  pub fn find_pos(
    &self,
    key: StaticKeyRef,
  ) -> Result<std::result::Result<(usize, Pointer), Pointer>> {
    if let Some((right, range)) = &self.right {
      if self.page.range(range.clone()) <= key {
        return Ok(Err(*right));
      };
    }

    let mut scanner = self.page.scanner();
    scanner.advance(self.offset).unwrap();

    let mut prev_child = scanner.read_u64()?;
    for i in 0..self.len {
      let l = scanner.read_u16()? as usize;
      let offset = scanner.advance(l)?;
      let k = self.page.range(offset..(offset + l));
      let child = scanner.read_u64()?;
      if k < key {
        prev_child = child;
        continue;
      } else if k == key {
        return Ok(Ok((i + 1, child)));
      } else {
        return Ok(Ok((i, prev_child)));
      }
    }
    Ok(Ok((self.len, prev_child)))
  }
  pub fn find(&self, key: StaticKeyRef) -> Result<std::result::Result<Pointer, Pointer>> {
    Ok(match self.find_pos(key)? {
      Ok((_, ptr)) => Ok(ptr),
      Err(ptr) => Err(ptr),
    })
  }
  pub fn first_child(&self) -> Result<Pointer> {
    self.nth_child(0)
  }
  pub fn last_child(&self) -> Result<Pointer> {
    self.nth_child(self.len)
  }
  pub fn nth_child(&self, pos: usize) -> Result<Pointer> {
    let mut scanner = self.page.scanner();
    scanner.advance(self.offset).unwrap();

    let mut child = scanner.read_u64()?;
    for _ in 0..pos {
      let l = scanner.read_u16()? as usize;
      scanner.advance(l)?;
      child = scanner.read_u64()?;
    }
    Ok(child)
  }

  pub const fn len(&self) -> usize {
    self.len
  }
  pub fn get_right(&self) -> Option<(StaticKey, Pointer)> {
    self
      .right
      .as_ref()
      .map(|(p, range)| (self.page.copy_range(range.clone()), *p))
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
