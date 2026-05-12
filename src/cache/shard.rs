use std::{
  borrow::Borrow,
  collections::VecDeque,
  hash::{BuildHasher, Hash},
  mem::MaybeUninit,
};

use hashbrown::{raw::RawTable, Equivalent};

use crate::utils::{UnsafeBorrow, UnsafeBorrowMut};

const fn equivalent<'a, K, V, Q: ?Sized + Equivalent<K>>(
  key: &'a Q,
) -> impl Fn(&*mut CacheEntry<K, V>) -> bool + 'a {
  move |ptr| key.equivalent(ptr.borrow_unsafe().get_key())
}

const fn make_hasher<'a, K, V, S>(
  hash_builder: &'a S,
) -> impl Fn(&*mut CacheEntry<K, V>) -> u64 + 'a
where
  K: Hash,
  S: BuildHasher,
{
  move |ptr| hash_builder.hash_one(ptr.borrow_unsafe().get_key())
}

enum State {
  Small { freq: u8 },
  Main { freq: u8 },
  Ghost,
  Tombstone,
}

struct CacheEntry<K, V> {
  key: K,
  value: MaybeUninit<V>,
  state: State,
}
impl<K, V> CacheEntry<K, V> {
  fn get_key(&self) -> &K {
    &self.key
  }

  fn new_small(key: K) -> Self {
    Self {
      key,
      value: MaybeUninit::uninit(),
      state: State::Small { freq: 0 },
    }
  }
  fn new_main(key: K) -> Self {
    Self {
      key,
      value: MaybeUninit::uninit(),
      state: State::Main { freq: 0 },
    }
  }
  fn get_state(&self) -> &State {
    &self.state
  }
  fn get_state_mut(&mut self) -> &mut State {
    &mut self.state
  }
}

const MAX_FREQ: u8 = 3;

/**
 * A single shard of the block cache.
 * It implements a S3-FIFO algorithm, which divides entries into three
 * categories: small, main, and ghost.
 * New entries are inserted into the small category, and if the small
 * category exceeds its capacity, entries with frequency 1 or less are
 * moved to the main category, while entries with frequency greater than
 * 1 are evicted.
 * If the main category exceeds its capacity, entries with frequency 0 are
 * evicted.
 * The ghost category tracks recently evicted entries, and if the total
 * number of entries in the table exceeds the sum of small and main
 * capacities, entries in the ghost category are evicted until the total
 * number of entries is within the limit.
 */
pub struct CacheShard<K, V> {
  table: RawTable<*mut CacheEntry<K, V>>,
  small: VecDeque<*mut CacheEntry<K, V>>,
  main: VecDeque<*mut CacheEntry<K, V>>,
  ghost: VecDeque<*mut CacheEntry<K, V>>,
  small_cap: usize,
  small_count: usize,
  main_cap: usize,
  main_count: usize,
  ghost_cap: usize,
}
impl<K, V> CacheShard<K, V>
where
  K: Eq + Hash,
{
  pub fn new(capacity: usize) -> Self {
    let small_cap = capacity / 10;
    let main_cap = capacity - small_cap;
    Self {
      table: RawTable::new(),
      small: VecDeque::with_capacity(small_cap),
      main: VecDeque::with_capacity(main_cap),
      ghost: VecDeque::with_capacity(main_cap),
      small_cap,
      small_count: 0,
      main_cap,
      main_count: 0,
      ghost_cap: main_cap,
    }
  }
  pub fn peek<Q: ?Sized>(&mut self, key: &Q, hash: u64) -> Option<&V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    let entry = self.table.get(hash, equivalent(key))?.borrow_unsafe();
    match entry.get_state() {
      State::Main { .. } | State::Small { .. } => {
        Some(unsafe { entry.value.assume_init_ref() })
      }
      State::Ghost => None,
      State::Tombstone => unreachable!(),
    }
  }

  pub fn get<Q: ?Sized>(&mut self, key: &Q, hash: u64) -> Option<&V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    let entry = self
      .table
      .get_mut(hash, equivalent(key))?
      .borrow_mut_unsafe();
    match &mut entry.state {
      State::Small { freq } | State::Main { freq } => {
        *freq = (*freq + 1).min(MAX_FREQ);
        Some(unsafe { entry.value.assume_init_ref() })
      }
      State::Ghost | State::Tombstone => None,
    }
  }

  pub fn reserve_key<S, R, F>(
    &mut self,
    key: K,
    hash: u64,
    hash_builder: &S,
    evict: F,
  ) -> std::result::Result<Reserved<K, V, R>, ()>
  where
    K: Clone,
    S: BuildHasher,
    F: FnOnce(&V) -> Option<R>,
  {
    match self.table.find_or_find_insert_slot(
      hash,
      equivalent(&key),
      make_hasher(hash_builder),
    ) {
      Ok(bucket) => {
        let entry = unsafe { bucket.as_ref() }.borrow_mut_unsafe();
        match entry.get_state_mut() {
          State::Small { .. } | State::Main { .. } => {
            unreachable!("must check before reserve")
          }
          State::Ghost => {
            let evicted = self.evict_main(hash_builder, evict)?;

            let new_entry = CacheEntry::new_main(key);
            let ptr = Box::into_raw(Box::new(new_entry));
            let old = unsafe { bucket.as_ptr().replace(ptr) };
            old.borrow_mut_unsafe().state = State::Tombstone;

            self.main.push_back(ptr);
            self.main_count += 1;
            Ok(Reserved::new(
              evicted,
              ptr.borrow_mut_unsafe().value.as_mut_ptr(),
            ))
          }
          State::Tombstone => unreachable!(),
        }
      }
      Err(slot) => {
        let evicted = self.evict_small(hash_builder, evict)?;

        let entry = CacheEntry::new_small(key);
        let ptr = Box::into_raw(Box::new(entry));
        unsafe { self.table.insert_in_slot(hash, slot, ptr) };

        self.small.push_back(ptr);
        self.small_count += 1;
        Ok(Reserved::new(
          evicted,
          ptr.borrow_mut_unsafe().value.as_mut_ptr(),
        ))
      }
    }
  }

  fn evict_small<S, R, F>(
    &mut self,
    hasher: &S,
    evict: F,
  ) -> std::result::Result<Option<(K, V, R)>, ()>
  where
    K: Clone,
    S: BuildHasher,
    F: FnOnce(&V) -> Option<R>,
  {
    while self.small_count >= self.small_cap {
      let ptr = self.small.pop_front().unwrap_or_else(|| unreachable!());
      let entry = ptr.borrow_mut_unsafe();
      match entry.get_state() {
        State::Small { freq } => {
          if *freq > 1 {
            let evicted = match self.evict_main(hasher, evict) {
              Ok(v) => v,
              Err(_) => {
                self.small.push_back(ptr);
                return Err(());
              }
            };

            entry.state = State::Main { freq: 0 };
            self.main.push_back(ptr);
            self.small_count -= 1;
            self.main_count += 1;

            return Ok(evicted);
          } else {
            let reserved = match evict(unsafe { entry.value.assume_init_ref() }) {
              Some(v) => v,
              None => {
                self.small.push_back(ptr);
                return Err(());
              }
            };

            let value = unsafe { entry.value.assume_init_read() };
            entry.state = State::Ghost;
            self.evict_ghost(hasher);
            self.ghost.push_back(ptr);
            self.small_count -= 1;
            return Ok(Some((entry.get_key().clone(), value, reserved)));
          }
        }
        State::Tombstone => {
          let _ = unsafe { Box::from_raw(ptr) };
          continue;
        }
        State::Ghost | State::Main { .. } => unreachable!(),
      }
    }

    Ok(None)
  }
  fn evict_main<S, R, F>(
    &mut self,
    hasher: &S,
    evict: F,
  ) -> std::result::Result<Option<(K, V, R)>, ()>
  where
    S: BuildHasher,
    F: FnOnce(&V) -> Option<R>,
  {
    while self.main_count >= self.main_cap {
      let ptr = match self.main.pop_front() {
        Some(v) => v,
        None => unreachable!(),
      };

      match ptr.borrow_mut_unsafe().get_state_mut() {
        State::Main { freq } => {
          if *freq > 0 {
            *freq -= 1;
            self.main.push_back(ptr);
            continue;
          } else {
            let reserved =
              match evict(unsafe { ptr.borrow_unsafe().value.assume_init_ref() }) {
                Some(v) => v,
                None => {
                  self.main.push_back(ptr);
                  return Err(());
                }
              };
            let entry = unsafe { Box::from_raw(ptr) };
            let key = entry.key;
            self
              .table
              .remove_entry(hasher.hash_one(&key), equivalent(&key));
            self.main_count -= 1;
            return Ok(Some((
              key,
              unsafe { entry.value.assume_init_read() },
              reserved,
            )));
          }
        }
        State::Ghost | State::Small { .. } => unreachable!(),
        State::Tombstone => {
          let _ = unsafe { Box::from_raw(ptr) };
          continue;
        }
      }
    }

    Ok(None)
  }
  fn evict_ghost<S>(&mut self, hasher: &S)
  where
    S: BuildHasher,
  {
    while self.table.len() - self.len() >= self.ghost_cap {
      let ptr = self.ghost.pop_front().unwrap_or_else(|| unreachable!());
      match ptr.borrow_unsafe().get_state() {
        State::Small { .. } => unreachable!(),
        State::Main { .. } => {}
        State::Ghost => {
          let entry = unsafe { Box::from_raw(ptr) };
          let key = entry.get_key();
          self
            .table
            .remove_entry(hasher.hash_one(key), equivalent(key));
        }
        State::Tombstone => {
          let _ = unsafe { Box::from_raw(ptr) };
        }
      }
    }
  }

  pub fn remove<Q: ?Sized>(&mut self, key: &Q, hash: u64) -> Option<V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    let entry = self
      .table
      .remove_entry(hash, equivalent(key))?
      .borrow_mut_unsafe();
    match entry.get_state_mut() {
      State::Main { .. } => {
        let value = unsafe { entry.value.assume_init_read() };
        entry.state = State::Tombstone;
        self.main_count -= 1;
        Some(value)
      }
      State::Small { .. } => {
        let value = unsafe { entry.value.assume_init_read() };
        entry.state = State::Tombstone;
        self.small_count -= 1;
        Some(value)
      }
      State::Ghost => {
        entry.state = State::Tombstone;
        None
      }
      State::Tombstone => unreachable!(),
    }
  }

  pub const fn len(&self) -> usize {
    self.main_count + self.small_count
  }
}

impl<K, V> Drop for CacheShard<K, V> {
  fn drop(&mut self) {
    for ptr in self
      .main
      .drain(..)
      .chain(self.small.drain(..))
      .chain(self.ghost.drain(..))
      .filter(|ptr| matches!(ptr.borrow_unsafe().get_state(), State::Tombstone))
    {
      let _ = unsafe { Box::from_raw(ptr) };
    }
    for ptr in self.table.drain() {
      if matches!(
        ptr.borrow_unsafe().get_state(),
        State::Small { .. } | State::Main { .. }
      ) {
        unsafe { ptr.borrow_mut_unsafe().value.assume_init_drop() };
      }
      let _ = unsafe { Box::from_raw(ptr) };
    }
  }
}

pub struct Reserved<K, V, R> {
  evicted: Option<(K, V, R)>,
  value: *mut V,
}
impl<K, V, R> Reserved<K, V, R> {
  const fn new(evicted: Option<(K, V, R)>, value: *mut V) -> Self {
    Self { evicted, value }
  }

  pub const fn take_evicted(&mut self) -> Option<(K, V, R)> {
    self.evicted.take()
  }

  pub const fn fulfill(&mut self, value: V) {
    unsafe { self.value.write(value) };
  }
}

#[cfg(test)]
#[path = "tests/shard.rs"]
mod tests;
