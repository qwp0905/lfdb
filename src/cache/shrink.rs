use std::{
  collections::{vec_deque::Drain, VecDeque},
  hash::{BuildHasher, Hash, RandomState},
  mem::replace,
  ops::{Deref, DerefMut, RangeBounds},
};

use hashbrown::{hash_table::Entry, Equivalent, HashTable};

pub struct ShrinkTable<T>(HashTable<T>);
impl<T> ShrinkTable<T> {
  pub const fn new() -> Self {
    Self(HashTable::new())
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
}
impl<T> Deref for ShrinkTable<T> {
  type Target = HashTable<T>;

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
  pub fn insert<S>(&mut self, key: K, hash: u64, hasher: &S)
  where
    K: Hash + Eq,
    S: BuildHasher,
  {
    let eq = Self::equivalent(&key);
    let hasher = Self::make_hasher(hasher);
    self.0.entry(hash, eq, hasher).or_insert(key);
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
      Entry::Occupied(mut entry) => Some(replace(&mut entry.get_mut().1, value)),
      Entry::Vacant(entry) => {
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
    Some(&self.table.find(hash, eq)?.1)
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
  pub fn drain<R: RangeBounds<usize>>(&mut self, range: R) -> Drain<'_, T> {
    self.0.drain(range)
  }
}
