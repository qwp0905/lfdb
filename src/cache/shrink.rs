use std::{
  collections::{vec_deque, VecDeque},
  hash::{BuildHasher, Hash, RandomState},
  mem::replace,
  ops::RangeBounds,
};

use hashbrown::{hash_table, Equivalent, HashTable};

pub struct ShrinkTable<T>(HashTable<T>);
impl<T> ShrinkTable<T> {
  pub const fn new() -> Self {
    Self(HashTable::new())
  }

  pub fn find(&self, hash: u64, eq: impl FnMut(&T) -> bool) -> Option<&T> {
    self.0.find(hash, eq)
  }

  pub fn find_entry(
    &mut self,
    hash: u64,
    eq: impl FnMut(&T) -> bool,
  ) -> std::result::Result<hash_table::OccupiedEntry<'_, T>, hash_table::AbsentEntry<'_, T>>
  {
    self.0.find_entry(hash, eq)
  }

  pub fn len(&self) -> usize {
    self.0.len()
  }

  pub fn insert_unique(
    &mut self,
    hash: u64,
    value: T,
    hasher: impl Fn(&T) -> u64,
  ) -> hash_table::OccupiedEntry<'_, T> {
    self.0.insert_unique(hash, value, hasher)
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

    let Ok(entry) = self.0.find_entry(hash, eq) else {
      return None;
    };
    let (v, _) = entry.remove();

    let cap = self.0.capacity();
    let threshold = (cap >> 3) * 3;
    if self.0.len() >= threshold {
      return Some(v);
    }
    self.0.shrink_to(cap >> 1, hasher);
    Some(v)
  }

  pub fn drain(&mut self) -> hash_table::Drain<'_, T> {
    self.0.drain()
  }

  fn entry(
    &mut self,
    hash: u64,
    eq: impl FnMut(&T) -> bool,
    hasher: impl Fn(&T) -> u64,
  ) -> hash_table::Entry<'_, T> {
    self.0.entry(hash, eq, hasher)
  }

  fn iter(&self) -> hash_table::Iter<'_, T> {
    self.0.iter()
  }
}

pub struct ShrinkSet<K>(ShrinkTable<K>);
impl<K> ShrinkSet<K> {
  const fn equivalent<'a, Q: ?Sized + Equivalent<K>>(
    key: &'a Q,
  ) -> impl Fn(&K) -> bool + 'a {
    |k| key.equivalent(k)
  }
  const fn make_hasher<'a, S: BuildHasher>(build_hasher: &'a S) -> impl Fn(&K) -> u64 + 'a
  where
    K: Hash,
  {
    |k| build_hasher.hash_one(k)
  }

  pub const fn new() -> Self {
    Self(ShrinkTable::new())
  }
  pub fn contains<Q>(&self, hash: u64, key: &Q) -> bool
  where
    Q: Equivalent<K> + ?Sized,
  {
    let eq = Self::equivalent(key);
    self.0.find(hash, eq).is_some()
  }
  pub fn insert_unchecked<S>(&mut self, key: K, hash: u64, hasher: &S)
  where
    K: Hash,
    S: BuildHasher,
  {
    let hasher = Self::make_hasher(hasher);
    self.0.insert_unique(hash, key, hasher);
  }
  pub fn remove<Q, S>(&mut self, hash: u64, key: &Q, hasher: &S)
  where
    Q: Equivalent<K> + ?Sized,
    K: Hash,
    S: BuildHasher,
  {
    let eq = Self::equivalent(key);
    let hasher = Self::make_hasher(hasher);
    self.0.remove_and_shrink(hash, eq, hasher);
  }
}

pub struct ShrinkMap<K, V> {
  table: ShrinkTable<(K, V)>,
  hasher: RandomState,
}
impl<K, V> ShrinkMap<K, V> {
  const fn equivalent<'a, Q: ?Sized + Equivalent<K>>(
    key: &'a Q,
  ) -> impl Fn(&(K, V)) -> bool + 'a {
    |(k, _)| key.equivalent(k)
  }
  const fn make_hasher<'a, S: BuildHasher>(
    build_hasher: &'a S,
  ) -> impl Fn(&(K, V)) -> u64 + 'a
  where
    K: Hash,
  {
    |(k, _)| build_hasher.hash_one(k)
  }

  pub fn new() -> Self {
    Self {
      table: ShrinkTable::new(),
      hasher: RandomState::new(),
    }
  }
  pub fn insert(&mut self, key: K, value: V) -> Option<V>
  where
    K: Hash + Eq,
  {
    let hash = self.hasher.hash_one(&key);
    let eq = Self::equivalent(&key);
    let hasher = Self::make_hasher(&self.hasher);
    match self.table.entry(hash, eq, hasher) {
      hash_table::Entry::Occupied(mut entry) => {
        let (_, old) = entry.get_mut();
        Some(replace(old, value))
      }
      hash_table::Entry::Vacant(entry) => {
        entry.insert((key, value));
        None
      }
    }
  }
  pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
  where
    Q: Equivalent<K> + Hash + ?Sized,
    K: Hash,
  {
    let hash = self.hasher.hash_one(key);
    let eq = Self::equivalent(key);
    let hasher = Self::make_hasher(&self.hasher);
    let (_, v) = self.table.remove_and_shrink(hash, eq, hasher)?;
    Some(v)
  }

  pub fn get<Q>(&self, key: &Q) -> Option<&'_ V>
  where
    Q: Equivalent<K> + Hash + ?Sized,
  {
    let hash = self.hasher.hash_one(key);
    let eq = Self::equivalent(key);
    let (_, v) = self.table.find(hash, eq)?;
    Some(v)
  }

  pub fn values(&self) -> impl Iterator<Item = &'_ V> + '_ {
    self.table.iter().map(|(_, v)| v)
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
  pub fn drain<R: RangeBounds<usize>>(&mut self, range: R) -> vec_deque::Drain<'_, T> {
    self.0.drain(range)
  }
}
