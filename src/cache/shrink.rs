use std::{
  collections::{vec_deque::Drain, VecDeque},
  hash::{BuildHasher, Hash, RandomState},
  mem::replace,
  ops::{Deref, DerefMut, RangeBounds},
};

use hashbrown::{raw::RawTable, Equivalent};

pub struct ShrinkTable<T>(RawTable<T>);
impl<T> ShrinkTable<T> {
  pub const fn new() -> Self {
    Self(RawTable::new())
  }

  pub fn remove_and_shrink(
    &mut self,
    hash: u64,
    eq: impl FnMut(&T) -> bool,
    hasher: impl Fn(&T) -> u64,
  ) -> Option<T> {
    if self.0.is_empty() {
      return None;
    }
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
impl<T> Deref for ShrinkTable<T> {
  type Target = RawTable<T>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}
impl<T> DerefMut for ShrinkTable<T> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

pub struct ShrinkSet<K>(ShrinkTable<K>);
impl<K> ShrinkSet<K> {
  pub const fn new() -> Self {
    Self(ShrinkTable::new())
  }
  pub fn contains<Q>(&self, hash: u64, key: &Q) -> bool
  where
    Q: Equivalent<K> + ?Sized,
  {
    self.0.find(hash, |k| key.equivalent(k)).is_some()
  }
  pub fn insert<S>(&mut self, key: K, hash: u64, hasher: &S)
  where
    K: Hash,
    S: BuildHasher,
  {
    self.0.insert(hash, key, |k| hasher.hash_one(k));
  }
  pub fn remove<Q, S>(&mut self, hash: u64, key: &Q, hasher: &S)
  where
    Q: Equivalent<K> + ?Sized,
    K: Hash,
    S: BuildHasher,
  {
    self
      .0
      .remove_and_shrink(hash, |k| key.equivalent(k), |k| hasher.hash_one(k));
  }
}

pub struct ShrinkMap<K, V> {
  table: ShrinkTable<(K, V)>,
  hasher: RandomState,
}
impl<K, V> ShrinkMap<K, V> {
  pub fn new() -> Self {
    Self {
      table: ShrinkTable::new(),
      hasher: RandomState::new(),
    }
  }
  pub fn insert(&mut self, key: K, value: V) -> Option<V>
  where
    K: Hash + Equivalent<K>,
  {
    let hash = self.hasher.hash_one(&key);
    match self.table.find_or_find_insert_slot(
      hash,
      |(k, _)| key.equivalent(k),
      |(k, _)| self.hasher.hash_one(k),
    ) {
      Ok(bucket) => Some(replace(&mut unsafe { bucket.as_mut() }.1, value)),
      Err(slot) => {
        unsafe { self.table.insert_in_slot(hash, slot, (key, value)) };
        None
      }
    }
  }
  pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
  where
    Q: Equivalent<K> + Hash,
    K: Hash,
  {
    let hash = self.hasher.hash_one(key);
    let (_, v) = self.table.remove_and_shrink(
      hash,
      |(k, _)| key.equivalent(k),
      |(k, _)| self.hasher.hash_one(k),
    )?;
    Some(v)
  }

  pub fn get<Q>(&self, key: &Q) -> Option<&'_ V>
  where
    Q: Equivalent<K> + Hash,
  {
    let hash = self.hasher.hash_one(key);
    let bucket = self.table.find(hash, |(k, _)| key.equivalent(k))?;
    Some(unsafe { &bucket.as_ref().1 })
  }

  pub fn values(&self) -> impl Iterator<Item = &'_ V> + '_ {
    unsafe { self.table.iter().map(|bucket| &bucket.as_ref().1) }
  }
}
impl<K, V> Default for ShrinkMap<K, V> {
  fn default() -> Self {
    Self::new()
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
