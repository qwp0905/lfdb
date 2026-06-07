use std::{
  ops::Deref,
  sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
  },
};

// use crate::utils::Vector;

const SHIFT: usize = 6;
const MAX_BIT: usize = 1 << SHIFT;
const MASK: usize = MAX_BIT - 1;

/**
 * Lock-free bitmap backed by a fixed-size array of AtomicU64.
 */
pub struct AtomicBitmap {
  bits: Vec<AtomicU64>,
  len: AtomicUsize,
}
impl AtomicBitmap {
  pub fn new(capacity: usize) -> Self {
    let cap = (capacity + MASK) >> SHIFT;
    let mut bits = Vec::with_capacity(cap);
    bits.resize_with(cap, || AtomicU64::new(0));
    AtomicBitmap {
      bits,
      len: AtomicUsize::new(0),
    }
  }

  pub fn insert(&self, n: usize) -> bool {
    let i = n >> SHIFT;
    if i >= self.bits.len() {
      return false;
    };
    let j = n & MASK;
    let b = 1 << j;
    let prev = self.bits[i].fetch_or(b, Ordering::Release);
    if prev & b != 0 {
      return false;
    }

    self.len.fetch_add(1, Ordering::Relaxed);
    true
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
    if prev & b == 0 {
      return false;
    }

    self.len.fetch_sub(1, Ordering::Relaxed);
    true
  }

  pub fn len(&self) -> usize {
    self.len.load(Ordering::Relaxed)
  }

  pub fn static_iter(self: &Arc<Self>) -> BitmapIter<Arc<Self>> {
    BitmapIter::new(Arc::clone(self))
  }
  pub const fn iter(&self) -> BitmapIter<&'_ Self> {
    BitmapIter::new(self)
  }
}

pub struct BitmapIter<T> {
  inner: T,
  index: usize,
  remaining: u64,
}
impl<T> BitmapIter<T> {
  pub const fn new(inner: T) -> Self {
    Self {
      inner,
      index: 0,
      remaining: 0,
    }
  }
}
impl<T> Iterator for BitmapIter<T>
where
  T: Deref<Target = AtomicBitmap>,
{
  type Item = usize;

  fn next(&mut self) -> Option<Self::Item> {
    let bits = &(*self.inner).bits;
    while self.remaining == 0 {
      if self.index >= bits.len() {
        return None;
      }
      self.remaining = bits[self.index].load(Ordering::Acquire);
      self.index += 1;
    }

    let bit = self.remaining.trailing_zeros() as usize;
    self.remaining &= self.remaining - 1;
    Some(((self.index - 1) << SHIFT) + bit)
  }
}

pub struct OffsetBitmap {
  offset: u64,
  bits: Vec<u64>,
}
impl OffsetBitmap {
  const MASK: u64 = MASK as u64;
  pub fn new(offset: u64, capacity: u64) -> Self {
    let cap = ((capacity + Self::MASK) >> SHIFT) as usize;
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
    self.bits[i] |= 1 << (diff & Self::MASK);
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
    self.bits[i] & (1 << (diff & Self::MASK)) != 0
  }
}

#[cfg(test)]
#[path = "tests/bit.rs"]
mod tests;
