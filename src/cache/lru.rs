use std::{
  borrow::Borrow,
  hash::{BuildHasher, Hash},
  ptr::NonNull,
};

use crate::utils::{UnsafeBorrow, UnsafeBorrowMut, UnsafeDrop, UnsafeTake};

use super::{Bucket, LRUList};
use hashbrown::{raw::RawTable, Equivalent};

const fn equivalent<'a, K, V, Q: ?Sized + Equivalent<K>>(
  key: &'a Q,
) -> impl Fn(&NonNull<Bucket<K, V>>) -> bool + 'a {
  move |ptr| key.equivalent(ptr.borrow_unsafe().get_key())
}

const fn make_hasher<'a, K, V, S>(
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
 * The rebalance threshold maintains a 5:3 ratio (new:old).
 */
pub struct LRUShard<K, V> {
  entries: RawTable<NonNull<Bucket<K, V>>>,
  old_sub_list: LRUList<K, V>,
  new_sub_list: LRUList<K, V>,
  capacity: usize,
}

impl<K, V> LRUShard<K, V> {
  pub const fn new(capacity: usize) -> Self {
    Self {
      entries: RawTable::new(),
      old_sub_list: LRUList::new(),
      new_sub_list: LRUList::new(),
      capacity,
    }
  }
}
impl<K, V> LRUShard<K, V>
where
  K: Eq + Hash,
{
  pub fn get<Q: ?Sized>(&mut self, key: &Q, hash: u64) -> Option<&V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    let ptr = self.entries.get_mut(hash, equivalent(key))?;
    let bucket = ptr.borrow_mut_unsafe();
    if !bucket.promote() {
      return Some(bucket.get_value());
    }

    if bucket.is_new() {
      self.new_sub_list.move_to_head(ptr);
      return Some(bucket.get_value());
    }

    self.old_sub_list.remove(ptr);
    self.new_sub_list.push_head(ptr);
    bucket.set_is_new(true);
    self.rebalance();

    Some(bucket.get_value())
  }

  /**
   * Must be called after every insertion into the new segment to maintain
   * the 5:3 (new:old) ratio. Demotes entries from the tail of the new
   * segment to the head of the old segment until the ratio is restored.
   */
  const fn rebalance(&mut self) {
    while self.new_sub_list.len() * 3 > self.old_sub_list.len() * 5 {
      let mut ptr = match self.new_sub_list.pop_tail() {
        Some(ptr) => ptr,
        None => return,
      };
      self.old_sub_list.push_head(&mut ptr);
      unsafe { ptr.as_mut() }.set_is_new(false);
    }
  }

  pub fn evict<S>(&mut self, hasher: &S) -> Option<((K, V), u64)>
  where
    S: BuildHasher,
  {
    let key = self.old_sub_list.pop_tail()?.borrow_unsafe().get_key();
    let h = hasher.hash_one(key);
    let bucket = self
      .entries
      .remove_entry(h, equivalent(key))
      .map(|ptr| ptr.take_unsafe())?;
    self.rebalance();
    Some((bucket.take(), h))
  }

  pub fn insert<S>(&mut self, key: K, value: V, hash: u64, hash_builder: &S) -> Option<V>
  where
    S: BuildHasher,
  {
    let ptr = match self.entries.find_or_find_insert_slot(
      hash,
      equivalent(&key),
      make_hasher(hash_builder),
    ) {
      Ok(raw) => raw.as_ptr().borrow_mut_unsafe(),
      Err(slot) => {
        let mut ptr = Bucket::new_ptr(key, value);
        unsafe { self.entries.insert_in_slot(hash, slot, ptr) };
        self.old_sub_list.push_head(&mut ptr);
        return None;
      }
    };

    let bucket = ptr.borrow_mut_unsafe();
    if !bucket.promote() {
      return Some(bucket.set_value(value));
    }

    if bucket.is_new() {
      self.new_sub_list.move_to_head(ptr);
      return Some(bucket.set_value(value));
    };

    self.old_sub_list.remove(ptr);
    self.new_sub_list.push_head(ptr);
    bucket.set_is_new(true);
    self.rebalance();

    Some(bucket.set_value(value))
  }

  pub fn remove<Q>(&mut self, key: &Q, hash: u64) -> Option<V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    let mut ptr = self.entries.remove_entry(hash, equivalent(key))?;
    ptr
      .borrow_unsafe()
      .is_new()
      .then_some(&mut self.new_sub_list)
      .unwrap_or(&mut self.old_sub_list)
      .remove(&mut ptr);
    self.rebalance();
    let (_, value) = ptr.take_unsafe().take();
    Some(value)
  }

  pub const fn len(&self) -> usize {
    self.new_sub_list.len() + self.old_sub_list.len()
  }

  pub const fn is_full(&self) -> bool {
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
