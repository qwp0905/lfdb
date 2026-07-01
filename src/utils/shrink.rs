use std::{
  collections::{vec_deque::Drain, VecDeque},
  ops::{Deref, DerefMut, RangeBounds},
};

use hashbrown::raw::RawTable;

pub struct HashTable<T>(RawTable<T>);
impl<T> HashTable<T> {
  pub const fn new() -> Self {
    Self(RawTable::new())
  }

  pub fn remove_and_shrink(
    &mut self,
    hash: u64,
    eq: impl FnMut(&T) -> bool,
    hasher: impl Fn(&T) -> u64,
  ) -> Option<T> {
    let v = self.0.remove_entry(hash, eq)?;

    let cap = self.0.capacity();
    let threshold = (cap >> 3) * 3;
    if self.0.len() >= threshold {
      return Some(v);
    }
    self.0.shrink_to(cap >> 1, hasher);
    Some(v)
  }
}
impl<T> Deref for HashTable<T> {
  type Target = RawTable<T>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}
impl<T> DerefMut for HashTable<T> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

pub struct ShrinkQueue<T>(VecDeque<T>);
impl<T> ShrinkQueue<T> {
  pub const fn new() -> Self {
    Self(VecDeque::new())
  }
  pub fn push(&mut self, v: T) {
    self.0.push_back(v);
  }
  pub fn pop(&mut self) -> Option<T> {
    let v = self.0.pop_front()?;

    let cap = self.0.capacity();
    let threshold = (cap >> 3) * 3;
    if self.0.len() >= threshold {
      return Some(v);
    }
    self.0.shrink_to(cap >> 1);
    Some(v)
  }
  pub fn drain<R: RangeBounds<usize>>(&mut self, range: R) -> Drain<'_, T> {
    self.0.drain(range)
  }
}
