use std::sync::atomic::{AtomicU64, Ordering};

const SHIFT: usize = 6;
const MAX_BIT: usize = 1 << SHIFT;
const MASK: usize = MAX_BIT - 1;

/**
 * Lock-free bitmap backed by a fixed-size array of AtomicU64.
 */
pub struct Bitmap {
  bits: Vec<AtomicU64>,
}
impl Bitmap {
  pub fn new(capacity: usize) -> Self {
    let mut bits = Vec::with_capacity(capacity);
    for _ in 0..capacity {
      bits.push(AtomicU64::new(0))
    }
    Bitmap { bits }
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

  pub fn iter(&self) -> BitMapIter<'_> {
    BitMapIter::new(&self.bits)
  }
}

pub struct BitMapIter<'a> {
  bits: &'a [AtomicU64],
  index: usize,
  remaining: u64,
}
impl<'a> BitMapIter<'a> {
  pub fn new(bits: &'a Vec<AtomicU64>) -> Self {
    let remaining = bits.first().map(|i| i.load(Ordering::Acquire)).unwrap_or(0);
    Self {
      bits,
      index: 0,
      remaining,
    }
  }
}

impl<'a> Iterator for BitMapIter<'a> {
  type Item = usize;

  fn next(&mut self) -> Option<Self::Item> {
    while self.remaining == 0 {
      self.index += 1;
      if self.index >= self.bits.len() {
        return None;
      };
      self.remaining = self.bits[self.index].load(Ordering::Acquire);
    }

    let bit = self.remaining.trailing_zeros() as usize;
    self.remaining &= self.remaining - 1;
    Some((self.index << SHIFT) + bit)
  }
}

#[cfg(test)]
#[path = "tests/bit.rs"]
mod tests;
