use std::{
  iter::{repeat_n, Enumerate},
  sync::atomic::{AtomicU64, Ordering},
};

const SHIFT: u32 = u64::BITS.ilog2();
const MAX_BIT: u64 = 1 << SHIFT;
const MASK: u64 = MAX_BIT - 1;

/**
 * Fixed-capacity atomic bitmap with interior mutability.
 *
 * `AtomicBitmap` can be shared immutably while individual bits are inserted or
 * removed with atomic operations. Each bit represents one integer index within
 * the configured capacity.
 */
pub struct AtomicBitmap {
  bits: Vec<AtomicU64>,
}
impl AtomicBitmap {
  pub fn new(capacity: usize) -> Self {
    let cap = (capacity + MASK as usize) >> SHIFT;
    let mut bits = Vec::with_capacity(cap);
    bits.resize_with(cap, || AtomicU64::new(0));
    Self { bits }
  }

  pub fn insert(&self, n: u64) -> bool {
    let i = (n >> SHIFT) as usize;
    if i >= self.bits.len() {
      return false;
    };
    let j = n & MASK;
    let b = 1 << j;
    let prev = self.bits[i].fetch_or(b, Ordering::Release);
    prev & b == 0
  }

  pub fn contains(&self, n: u64) -> bool {
    let i = (n >> SHIFT) as usize;
    if i >= self.bits.len() {
      return false;
    };
    let j = n & MASK;
    self.bits[i].load(Ordering::Acquire) & (1 << j) != 0
  }

  pub fn remove(&self, n: u64) -> bool {
    let i = (n >> SHIFT) as usize;
    if i >= self.bits.len() {
      return false;
    };
    let j = n & MASK;
    let b = 1 << j;
    let prev = self.bits[i].fetch_and(!b, Ordering::Release);
    prev & b != 0
  }

  pub fn iter(&self) -> BitmapIter<impl Iterator<Item = u64> + '_> {
    let iter = self.bits.iter().map(|bit| bit.load(Ordering::Acquire));
    BitmapIter::new(iter, 0)
  }
}

/**
 * Loose iterator over an `AtomicBitmap`.
 *
 * The iterator does not create a consistent snapshot. Each word is loaded while
 * iteration progresses, so concurrent insert/remove operations may or may not
 * be observed. This is intended for callers that can tolerate a relaxed view of
 * the bitmap.
 */
pub struct BitmapIter<T> {
  iter: Enumerate<T>,
  remaining: u64,
  offset: u64,
  index: u64,
}
impl<T: Iterator<Item = u64>> BitmapIter<T> {
  fn new(iter: T, offset: u64) -> Self {
    Self {
      iter: iter.enumerate(),
      remaining: 0,
      offset,
      index: 0,
    }
  }
}
impl<T: Iterator<Item = u64>> Iterator for BitmapIter<T> {
  type Item = u64;

  fn next(&mut self) -> Option<Self::Item> {
    while self.remaining == 0 {
      let (index, bit) = self.iter.next()?;
      self.remaining = bit;
      self.index = index as u64;
    }

    let i = self.remaining.trailing_zeros() as u64;
    self.remaining &= self.remaining - 1;
    Some((self.index << SHIFT) + i + self.offset)
  }
}

/**
 * Non-atomic bitmap over an offset-based integer range.
 *
 * Plain bitmaps become wasteful when the represented values are large but the
 * interesting range is narrow. `OffsetBitmap` stores bits relative to a base
 * offset, so values in `offset..offset + capacity` can be represented compactly.
 *
 * The represented window grows on insert and shrinks from the front on removal.
 * An empty bitmap has no meaningful offset, so the next insert re-anchors the
 * window at the word of the inserted value.
 */
pub struct OffsetBitmap {
  offset: u64,
  bits: Vec<u64>,
}
impl OffsetBitmap {
  pub const fn new(offset: u64) -> Self {
    Self {
      offset,
      bits: Vec::new(),
    }
  }

  #[inline]
  fn locate(&self, n: u64) -> Option<(usize, u64)> {
    let diff = n.checked_sub(self.offset)?;
    let i = (diff >> SHIFT) as usize;
    (i < self.bits.len()).then(|| (i, 1 << (diff & MASK)))
  }

  #[inline]
  fn locate_or_extend(&mut self, n: u64) -> (usize, u64) {
    if self.bits.is_empty() {
      self.offset = n & !MASK;
      self.bits.push(0);
      return (0, 1 << (n & MASK));
    }
    if let Some(diff) = n.checked_sub(self.offset) {
      let i = (diff >> SHIFT) as usize;
      if i >= self.bits.len() {
        self.bits.resize(i + 1, 0);
      }
      return (i, 1 << (diff & MASK));
    }

    let offset = n & !MASK;
    let count = ((self.offset - offset) >> SHIFT) as usize;
    self.bits.splice(0..0, repeat_n(0, count));
    self.offset = offset;
    (0, 1 << (n & MASK))
  }

  #[inline]
  pub fn insert(&mut self, n: u64) -> bool {
    let (i, bit) = self.locate_or_extend(n);
    let old = self.bits[i];
    self.bits[i] |= bit;
    old & bit == 0
  }

  #[inline]
  pub fn contains(&self, n: u64) -> bool {
    self
      .locate(n)
      .is_some_and(|(i, bit)| self.bits[i] & bit != 0)
  }

  pub fn iter(&self) -> BitmapIter<impl Iterator<Item = u64> + '_> {
    BitmapIter::new(self.bits.iter().copied(), self.offset)
  }

  fn remove(&mut self, n: u64) -> bool {
    let Some((i, bit)) = self.locate(n) else {
      return false;
    };
    let old = self.bits[i];
    self.bits[i] &= !bit;
    old & bit != 0
  }

  fn move_and_shrink(&mut self, move_count: usize) {
    if move_count >= self.bits.len() {
      self.bits.clear();
      return;
    }
    if move_count > 0 {
      self.bits.copy_within(move_count.., 0);
      self.bits.truncate(self.bits.len() - move_count);
      if self.bits.capacity() >> 2 > self.bits.len() {
        self.bits.shrink_to(self.bits.len() << 1);
      }
      self.offset += (move_count as u64) << SHIFT;
    }
  }

  pub fn remove_and_shrink(&mut self, n: u64) -> bool {
    if !self.remove(n) {
      return false;
    }

    let move_count = self.bits.iter().take_while(|&&bit| bit == 0).count();
    self.move_and_shrink(move_count);
    true
  }

  pub fn remove_until(&mut self, n: u64) {
    let Some(diff) = n.checked_sub(self.offset) else {
      return;
    };

    let move_count = (diff >> SHIFT) as usize;
    if move_count >= self.bits.len() {
      self.bits.clear();
      return;
    }

    self.bits[move_count] &= u64::MAX << (diff & MASK);
    let zeros = self.bits[move_count..]
      .iter()
      .take_while(|&&bit| bit == 0)
      .count();
    self.move_and_shrink(move_count + zeros);
  }

  pub fn front(&self) -> Option<u64> {
    if self.bits.is_empty() {
      return None;
    };
    if self.bits[0] == 0 {
      return None;
    }
    Some(self.bits[0].trailing_zeros() as u64 + self.offset)
  }
}
impl Clone for OffsetBitmap {
  fn clone(&self) -> Self {
    Self {
      offset: self.offset,
      bits: self.bits.clone(),
    }
  }
}

pub struct AtomicSizedBitmap<const N: usize> {
  bits: [AtomicU64; N],
}
impl<const N: usize> AtomicSizedBitmap<N> {
  pub const fn new() -> Self {
    Self {
      bits: [const { AtomicU64::new(0) }; N],
    }
  }

  pub const fn calc_capacity() -> usize {
    (N + MASK as usize) >> SHIFT
  }

  pub fn insert(&self, n: u64) -> bool {
    let i = (n >> SHIFT) as usize;
    if i >= N {
      return false;
    };
    let j = n & MASK;
    let b = 1 << j;
    let prev = self.bits[i].fetch_or(b, Ordering::Release);
    prev & b == 0
  }

  pub fn contains(&self, n: u64) -> bool {
    let i = (n >> SHIFT) as usize;
    if i >= N {
      return false;
    };
    let j = n & MASK;
    self.bits[i].load(Ordering::Acquire) & (1 << j) != 0
  }
}

#[cfg(all(test, not(loom)))]
#[path = "tests/bit.rs"]
mod tests;
