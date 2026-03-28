use std::{
  iter::Enumerate,
  sync::atomic::{AtomicU64, Ordering},
};

const SHIFT: usize = 6;
const MAX_BIT: usize = 1 << SHIFT;
const MASK: usize = MAX_BIT - 1;

/**
 * Lock-free bitmap backed by a fixed-size array of AtomicU64.
 */
pub struct AtomicBitmap {
  bits: Vec<AtomicU64>,
}
impl AtomicBitmap {
  pub fn new(capacity: usize) -> Self {
    let cap = (capacity + MASK) >> SHIFT;
    let mut bits = Vec::with_capacity(cap);
    bits.resize_with(cap, || AtomicU64::new(0));
    AtomicBitmap { bits }
  }

  pub fn insert(&self, n: usize) -> bool {
    let i = n >> SHIFT;
    if i >= self.bits.len() {
      return false;
    };
    let j = n & MASK;
    let b = 1 << j;
    let prev = self.bits[i].fetch_or(b, Ordering::Release);
    prev & b == 0
  }

  pub fn contains(&self, n: usize) -> bool {
    let i = n >> SHIFT;
    if i >= self.bits.len() {
      return false;
    };
    let j = n & MASK;
    self.bits[i].load(Ordering::Acquire) & (1 << j) != 0
  }

  pub fn remove(&self, n: usize) -> bool {
    let i = n >> SHIFT;
    if i >= self.bits.len() {
      return false;
    };
    let j = n & MASK;
    let b = 1 << j;
    let prev = self.bits[i].fetch_and(!b, Ordering::Release);
    prev & b != 0
  }

  pub fn iter(&self) -> BitMapIter<impl Iterator<Item = u64> + '_> {
    BitMapIter::new(
      self
        .bits
        .iter()
        .map(|bit| bit.load(Ordering::Acquire))
        .enumerate(),
    )
  }
}

pub struct BitMapIter<I> {
  bits: Enumerate<I>,
  index: usize,
  remaining: u64,
}
impl<'a, I> BitMapIter<I>
where
  I: Iterator<Item = u64>,
{
  pub fn new(mut bits: Enumerate<I>) -> Self {
    let (index, remaining) = bits.next().unwrap_or((0, 0));
    Self {
      bits,
      index,
      remaining,
    }
  }
}

impl<'a, I> Iterator for BitMapIter<I>
where
  I: Iterator<Item = u64>,
{
  type Item = usize;

  fn next(&mut self) -> Option<Self::Item> {
    while self.remaining == 0 {
      match self.bits.next() {
        Some((i, bit)) => {
          self.remaining = bit;
          self.index = i;
        }
        None => return None,
      }
    }

    let bit = self.remaining.trailing_zeros() as usize;
    self.remaining &= self.remaining - 1;
    Some((self.index << SHIFT) + bit)
  }
}

pub struct OffsetBitmap {
  offset: usize,
  bits: Vec<u64>,
}
impl OffsetBitmap {
  pub fn new(offset: usize, capacity: usize) -> Self {
    let cap = (capacity + MASK) >> SHIFT;
    let bits = vec![0; cap];
    Self { offset, bits }
  }

  pub fn insert(&mut self, n: usize) -> bool {
    if n < self.offset {
      return false;
    }
    let diff = n - self.offset;

    let i = diff >> SHIFT;
    if i >= self.bits.len() {
      return false;
    };
    let old = self.bits[i];
    self.bits[i] |= 1 << (diff & MASK);
    old != self.bits[i]
  }

  pub fn contains(&self, n: usize) -> bool {
    if n < self.offset {
      return false;
    }
    let diff = n - self.offset;

    let i = diff >> SHIFT;
    if i >= self.bits.len() {
      return false;
    }
    self.bits[i] & (1 << (diff & MASK)) != 0
  }
}

#[cfg(test)]
#[path = "tests/bit.rs"]
mod tests;
