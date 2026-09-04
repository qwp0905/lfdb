use std::{
  iter::Enumerate,
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
 * The represented range is fixed when the bitmap is created. Callers do not
 * need to pre-check whether a value belongs to that range; values outside the
 * range simply cannot be inserted or found.
 */
pub struct OffsetBitmap {
  offset: u64,
  bits: Vec<u64>,
}
impl OffsetBitmap {
  pub fn new(offset: u64, capacity: u64) -> Self {
    let cap = ((capacity + MASK) >> SHIFT) as usize;
    let bits = vec![0; cap];
    Self { offset, bits }
  }

  #[inline]
  pub fn insert(&mut self, n: u64) -> bool {
    if n < self.offset {
      return false;
    }
    let diff = n - self.offset;

    let i = (diff >> SHIFT) as usize;
    if i >= self.bits.len() {
      return false;
    };
    let old = self.bits[i];
    self.bits[i] |= 1 << (diff & MASK);
    old != self.bits[i]
  }

  #[inline]
  pub fn contains(&self, n: u64) -> bool {
    if n < self.offset {
      return false;
    }
    let diff = n - self.offset;

    let i = (diff >> SHIFT) as usize;
    if i >= self.bits.len() {
      return false;
    }
    self.bits[i] & (1 << (diff & MASK)) != 0
  }

  pub fn iter(&self) -> BitmapIter<impl Iterator<Item = u64> + '_> {
    BitmapIter::new(self.bits.iter().copied(), self.offset)
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

#[cfg(test)]
#[path = "tests/bit.rs"]
mod tests;
