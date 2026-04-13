use std::{
  borrow::Borrow,
  hash::{BuildHasher, Hash},
  ptr::NonNull,
};

use crate::utils::{UnsafeBorrow, UnsafeBorrowMut, UnsafeDrop, UnsafeTake};

use super::{Bucket, LRUList};
use hashbrown::{raw::RawTable, Equivalent};

fn equivalent<'a, K, V, Q: ?Sized + Equivalent<K>>(
  key: &'a Q,
) -> impl Fn(&NonNull<Bucket<K, V>>) -> bool + 'a {
  move |ptr| key.equivalent(ptr.borrow_unsafe().get_key())
}

fn make_hasher<'a, K, V, S>(
  hash_builder: &'a S,
) -> impl Fn(&NonNull<Bucket<K, V>>) -> u64 + 'a
where
  K: Hash,
  S: BuildHasher,
{
  move |ptr| hash_builder.hash_one(ptr.borrow_unsafe().get_key())
}

/**
 * Two-segment LRU inspired by InnoDB's midpoint insertion strategy.
 * New entries are placed in the old segment first; they are promoted
 * to the new segment only on a subsequent access. This makes the cache
 * resistant to large sequential scans — pages read only once never
 * displace frequently accessed (hot) pages from the new segment.
 *
 * The rebalance threshold maintains a 3:5 ratio (new:old).
 */
pub struct LRUShard<K, V> {
  old_entries: RawTable<NonNull<Bucket<K, V>>>,
  old_sub_list: LRUList<K, V>,
  new_entries: RawTable<NonNull<Bucket<K, V>>>,
  new_sub_list: LRUList<K, V>,
  capacity: usize,
}

impl<K, V> LRUShard<K, V> {
  pub fn new(capacity: usize) -> Self {
    Self {
      old_entries: RawTable::new(),
      old_sub_list: LRUList::new(),
      new_entries: RawTable::new(),
      new_sub_list: LRUList::new(),
      capacity,
    }
  }
}
impl<K, V> LRUShard<K, V>
where
  K: Eq + Hash,
{
  #[inline]
  fn promote_bucket<Q: ?Sized, S>(
    &mut self,
    key: &Q,
    hash: u64,
    hasher: &S,
  ) -> Option<&mut Bucket<K, V>>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
    S: BuildHasher,
  {
    if let Some(ptr) = self.new_entries.get_mut(hash, equivalent(key)) {
      let bucket = ptr.borrow_mut_unsafe();
      self.new_sub_list.move_to_head(ptr);
      return Some(bucket);
    }

    let ptr = self.old_entries.get_mut(hash, equivalent(key))?;
    let bucket = ptr.borrow_mut_unsafe();

    self.old_sub_list.remove(ptr);
    self.new_sub_list.push_head(ptr);
    self.new_entries.insert(hash, *ptr, make_hasher(hasher));
    self.old_entries.remove_entry(hash, equivalent(key));
    self.rebalance(hasher);
    Some(bucket)
  }

  pub fn get<Q: ?Sized, S>(&mut self, key: &Q, hash: u64, hasher: &S) -> Option<&V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
    S: BuildHasher,
  {
    Some(self.promote_bucket(key, hash, hasher)?.get_value())
  }

  pub fn peek<Q: ?Sized>(&self, key: &Q, hash: u64) -> Option<&V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    self
      .new_entries
      .get(hash, equivalent(key))
      .or_else(|| self.old_entries.get(hash, equivalent(key)))
      .map(|bucket| bucket.borrow_unsafe().get_value())
  }

  /**
   * Must be called after every insertion into the new segment to maintain
   * the 3:5 (new:old) ratio. Demotes entries from the tail of the new
   * segment to the head of the old segment until the ratio is restored.
   */
  fn rebalance<S>(&mut self, hasher: &S)
  where
    S: BuildHasher,
  {
    while self.new_sub_list.len() * 3 > self.old_sub_list.len() * 5 {
      let key = match self.new_sub_list.pop_tail() {
        Some(bucket) => bucket.borrow_unsafe().get_key(),
        None => break,
      };
      let h = hasher.hash_one(key);
      let mut bucket = self.new_entries.remove_entry(h, equivalent(key)).unwrap();
      self.old_sub_list.push_head(&mut bucket);
      self.old_entries.insert(h, bucket, make_hasher(hasher));
    }
  }

  pub fn evict<S>(&mut self, hasher: &S) -> Option<((K, V), u64)>
  where
    S: BuildHasher,
  {
    let key = self.old_sub_list.pop_tail()?.borrow_unsafe().get_key();
    let h = hasher.hash_one(key);
    let bucket = self
      .old_entries
      .remove_entry(h, equivalent(key))
      .map(|ptr| ptr.take_unsafe())?;
    self.rebalance(hasher);
    Some((bucket.take(), h))
  }

  pub fn insert<S>(&mut self, key: K, value: V, hash: u64, hasher: &S) -> Option<V>
  where
    S: BuildHasher,
  {
    if let Some(bucket) = self.promote_bucket(&key, hash, hasher) {
      return Some(bucket.set_value(value));
    }

    let mut bucket = Bucket::new_ptr(key, value);

    // Insert into the head of the old segment, not the tail.
    // This gives the page a chance to be promoted to the new segment
    // on a subsequent access before being evicted — the core of
    // midpoint insertion.
    self.old_sub_list.push_head(&mut bucket);
    self.old_entries.insert(hash, bucket, make_hasher(hasher));
    None
  }

  pub fn remove<Q, S>(&mut self, key: &Q, hash: u64, hasher: &S) -> Option<V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
    S: BuildHasher,
  {
    if let Some(mut bucket) = self.new_entries.remove_entry(hash, equivalent(key)) {
      self.new_sub_list.remove(&mut bucket);
      let (_, value) = bucket.take_unsafe().take();
      return Some(value);
    }

    let mut bucket = match self.old_entries.remove_entry(hash, equivalent(key)) {
      Some(bucket) => bucket,
      None => return None,
    };

    self.old_sub_list.remove(&mut bucket);
    self.rebalance(hasher);
    let (_, value) = bucket.take_unsafe().take();
    Some(value)
  }

  // pub fn has<Q: ?Sized>(&self, key: &Q, hash: u64) -> bool
  // where
  //   K: Borrow<Q>,
  //   Q: Hash + Eq,
  // {
  //   self.new_entries.find(hash, equivalent(key)).is_some()
  //     || self.old_entries.find(hash, equivalent(key)).is_some()
  // }

  pub fn len(&self) -> usize {
    self.new_entries.len() + self.old_entries.len()
  }

  pub fn is_full(&self) -> bool {
    self.len() == self.capacity
  }
}

impl<K, V> Drop for LRUShard<K, V> {
  fn drop(&mut self) {
    while let Some(ptr) = self.old_sub_list.pop_tail() {
      ptr.drop_unsafe();
    }
    while let Some(ptr) = self.new_sub_list.pop_tail() {
      ptr.drop_unsafe();
    }
  }
}

#[cfg(test)]
#[path = "tests/lru.rs"]
mod tests;
