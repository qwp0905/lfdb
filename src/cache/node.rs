use std::{
  borrow::Borrow,
  collections::VecDeque,
  hash::{BuildHasher, Hash},
  mem::MaybeUninit,
};

use hashbrown::{raw::RawTable, Equivalent};

use crate::utils::{UnsafeBorrow, UnsafeBorrowMut, UnsafeDrop, UnsafeTake};

const fn equivalent<'a, K, V, Q: ?Sized + Equivalent<K>>(
  key: &'a Q,
) -> impl Fn(&*mut CacheEntry<K, V>) -> bool + 'a {
  move |ptr| key.equivalent(ptr.borrow_unsafe().get_key())
}

const fn ptr_eq<K, V>(
  ptr: *mut CacheEntry<K, V>,
) -> impl Fn(&*mut CacheEntry<K, V>) -> bool {
  move |p| ptr == *p
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
  Small {
    freq: u8,
  },
  Main {
    freq: u8,
  },
  Ghost,

  /**
   * Removed from the lookup table and no longer owns a live value, but the raw
   * pointer may still be present in one of the FIFO queues. It is freed lazily
   * when that queue entry reaches the front.
   */
  Tombstone,
}

struct CacheEntry<K, V> {
  key: K,
  value: MaybeUninit<V>,
  state: State,
}
impl<K, V> CacheEntry<K, V> {
  const fn get_key(&self) -> &K {
    &self.key
  }

  const fn new_small(key: K) -> Self {
    Self {
      key,
      value: MaybeUninit::uninit(),
      state: State::Small { freq: 0 },
    }
  }
  const fn new_main(key: K) -> Self {
    Self {
      key,
      value: MaybeUninit::uninit(),
      state: State::Main { freq: 0 },
    }
  }
  const fn get_state(&self) -> &State {
    &self.state
  }
  const fn get_state_mut(&mut self) -> &mut State {
    &mut self.state
  }
  const fn set_state(&mut self, state: State) {
    self.state = state;
  }
  const fn get_value(&self) -> &'_ V {
    unsafe { self.value.assume_init_ref() }
  }
  const fn take_value(&self) -> V {
    unsafe { self.value.assume_init_read() }
  }
  const fn value_ptr(&mut self) -> *mut V {
    self.value.as_mut_ptr()
  }
  fn drop_value(&mut self) {
    unsafe { self.value.assume_init_drop() };
  }
  fn take_key(self) -> K {
    self.key
  }
}

const MAX_FREQ: u8 = 3;

/**
 * S3-FIFO cache node.
 *
 * A node keeps three FIFO queues:
 * - `small`: probationary entries for newly inserted keys.
 * - `main`: protected entries promoted from `small` after enough hits.
 * - `ghost`: recently evicted keys whose values have been dropped.
 *
 * `RawTable` is the lookup index for entries that live in any of those queues.
 * A hit increments a small saturating frequency counter. When the cache needs
 * space, `small` entries with enough frequency are promoted to `main`; the rest
 * lose their value and become ghost entries. A later hit on a ghost entry
 * reserves a new value slot directly in `main`.
 */
pub struct CacheNode<K, V> {
  table: RawTable<*mut CacheEntry<K, V>>,
  small: VecDeque<*mut CacheEntry<K, V>>,
  main: VecDeque<*mut CacheEntry<K, V>>,
  ghost: VecDeque<*mut CacheEntry<K, V>>,
  capacity: usize,
  small_cap: usize,
  small_count: usize,
  main_count: usize,
  ghost_cap: usize,
}
impl<K, V> CacheNode<K, V>
where
  K: Eq + Hash,
{
  pub const fn new(capacity: usize) -> Self {
    // S3-FIFO's usual split: a small probationary queue and a large protected
    // main queue. This ratio is kept as the recommended/default value; tuning it
    // did not show meaningful performance changes here.
    let small_cap = capacity / 10;
    let main_cap = capacity - small_cap;
    Self {
      table: RawTable::new(),
      small: VecDeque::new(),
      main: VecDeque::new(),
      ghost: VecDeque::new(),
      capacity,
      small_cap,
      small_count: 0,
      main_count: 0,
      ghost_cap: main_cap,
    }
  }

  pub fn reserve<S, R, F>(
    &mut self,
    key: &K,
    hash: u64,
    hash_builder: &S,
    try_evict: F,
  ) -> std::result::Result<Reserved<K, V, R>, ()>
  where
    K: Clone,
    S: BuildHasher,
    F: Fn(&V) -> Option<R>,
  {
    debug_assert!(self.table.find(hash, equivalent(key)).is_none());
    let evicted = self.evict(hash_builder, &try_evict)?;

    let ptr = Box::into_raw(Box::new(CacheEntry::new_small(key.clone())));
    self.table.insert(hash, ptr, make_hasher(hash_builder));

    self.small.push_back(ptr);
    self.small_count += 1;
    Ok(Reserved::new(evicted, ptr.borrow_mut_unsafe().value_ptr()))
  }

  /**
   * `try_evict` is the external eviction gate. Before inserting a new entry into
   * the raw table, the cache first proves that some live entry can actually be
   * removed; this avoids growing the raw table just because all current victims
   * are temporarily unevictable.
   */
  pub fn get_or_reserve<S, R, F>(
    &mut self,
    key: &K,
    hash: u64,
    hash_builder: &S,
    try_evict: F,
  ) -> std::result::Result<GetOrReserve<'_, K, V, R>, ()>
  where
    K: Clone,
    S: BuildHasher,
    F: Fn(&V) -> Option<R>,
  {
    let Some(bucket) = self.table.find(hash, equivalent(key)) else {
      return self
        .reserve(key, hash, hash_builder, try_evict)
        .map(GetOrReserve::Reserved);
    };

    let entry = unsafe { *bucket.as_ptr() }.borrow_mut_unsafe();
    match entry.get_state_mut() {
      State::Small { freq } | State::Main { freq } => {
        *freq = (*freq + 1).min(MAX_FREQ);
        Ok(GetOrReserve::Hit(entry.get_value()))
      }
      State::Ghost => {
        let (old, _) = unsafe { self.table.remove(bucket) };

        // Do not revive the ghost entry in place. Its pointer is still queued in the
        // ghost FIFO, so reusing it as a live entry would let that queue observe the
        // live entry again later. Leave the old pointer as a tombstone and insert a
        // fresh main entry instead.
        old.borrow_mut_unsafe().set_state(State::Tombstone);

        let evicted = self.evict(hash_builder, &try_evict)?;

        let ptr = Box::into_raw(Box::new(CacheEntry::new_main(key.clone())));
        self.table.insert(hash, ptr, make_hasher(hash_builder));
        self.main.push_back(ptr);
        self.main_count += 1;
        Ok(GetOrReserve::Reserved(Reserved::new(
          evicted,
          ptr.borrow_mut_unsafe().value_ptr(),
        )))
      }
      State::Tombstone => unreachable!(),
    }
  }

  fn evict<S, R, F>(
    &mut self,
    hasher: &S,
    try_evict: &F,
  ) -> std::result::Result<Option<(K, V, R)>, ()>
  where
    K: Clone,
    S: BuildHasher,
    F: Fn(&V) -> Option<R>,
  {
    while self.is_full() {
      if self.small_count > self.small_cap {
        return self.evict_small(hasher, try_evict).map(Some);
      }

      if let Some(v) = self.evict_main(hasher, try_evict)? {
        return Ok(Some(v));
      }
    }

    Ok(None)
  }

  fn evict_small<S, R, F>(
    &mut self,
    hasher: &S,
    try_evict: &F,
  ) -> std::result::Result<(K, V, R), ()>
  where
    K: Clone,
    S: BuildHasher,
    F: Fn(&V) -> Option<R>,
  {
    loop {
      let ptr = self.small.pop_front().unwrap();
      let entry = ptr.borrow_mut_unsafe();
      match entry.get_state() {
        State::Small { freq } if *freq > 1 => {
          let Ok(evicted) = self.evict_main(hasher, try_evict) else {
            self.small.push_back(ptr);
            return Err(());
          };

          entry.set_state(State::Main { freq: 0 });
          self.main.push_back(ptr);
          self.small_count -= 1;
          self.main_count += 1;

          if let Some(v) = evicted {
            return Ok(v);
          }
        }
        State::Small { .. } => {
          let Some(reserved) = try_evict(entry.get_value()) else {
            self.small.push_back(ptr);
            return Err(());
          };

          self.small_count -= 1;
          entry.set_state(State::Ghost);
          self.evict_ghost(hasher);
          self.ghost.push_back(ptr);
          return Ok((entry.get_key().clone(), entry.take_value(), reserved));
        }
        State::Tombstone => ptr.drop_unsafe(),
        State::Ghost | State::Main { .. } => unreachable!(),
      }
    }
  }
  fn evict_main<S, R, F>(
    &mut self,
    hasher: &S,
    try_evict: &F,
  ) -> std::result::Result<Option<(K, V, R)>, ()>
  where
    S: BuildHasher,
    F: Fn(&V) -> Option<R>,
  {
    while let Some(ptr) = self.main.pop_front() {
      let entry = ptr.borrow_mut_unsafe();
      match entry.get_state_mut() {
        State::Main { freq } if *freq > 0 => {
          *freq -= 1;
          self.main.push_back(ptr);
          continue;
        }
        State::Main { .. } => {
          let Some(reserved) = try_evict(entry.get_value()) else {
            self.main.push_back(ptr);
            return Err(());
          };

          self
            .table
            .remove_entry(hasher.hash_one(entry.get_key()), ptr_eq(ptr))
            .unwrap_or_else(|| unreachable!());

          let entry = ptr.take_unsafe();
          let value = entry.take_value();
          self.main_count -= 1;
          return Ok(Some((entry.take_key(), value, reserved)));
        }
        State::Ghost | State::Small { .. } => unreachable!(),
        State::Tombstone => ptr.drop_unsafe(),
      }
    }

    Ok(None)
  }
  fn evict_ghost<S>(&mut self, hasher: &S)
  where
    S: BuildHasher,
  {
    // Ghost entries stay in the lookup table but no longer count as live cache
    // values. Their count is `table.len() - len()`, so no separate ghost counter
    // is needed.
    while self.table.len() - self.len() >= self.ghost_cap {
      let ptr = self.ghost.pop_front().unwrap_or_else(|| unreachable!());
      let entry = ptr.borrow_unsafe();
      match entry.get_state() {
        State::Main { .. } | State::Small { .. } => unreachable!(),
        State::Ghost => {
          self
            .table
            .remove_entry(hasher.hash_one(entry.get_key()), ptr_eq(ptr))
            .unwrap_or_else(|| unreachable!());
          ptr.drop_unsafe();
        }
        State::Tombstone => ptr.drop_unsafe(),
      }
    }
  }

  #[cold]
  pub fn remove<Q>(&mut self, key: &Q, hash: u64) -> Option<V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    let entry = self
      .table
      .remove_entry(hash, equivalent(key))?
      .borrow_mut_unsafe();
    match entry.get_state_mut() {
      State::Main { .. } => {
        entry.set_state(State::Tombstone);
        self.main_count -= 1;
        Some(entry.take_value())
      }
      State::Small { .. } => {
        entry.set_state(State::Tombstone);
        self.small_count -= 1;
        Some(entry.take_value())
      }
      State::Ghost => {
        entry.set_state(State::Tombstone);
        None
      }
      State::Tombstone => unreachable!(),
    }
  }

  pub const fn len(&self) -> usize {
    self.main_count + self.small_count
  }

  const fn is_full(&self) -> bool {
    self.len() >= self.capacity
  }

  #[allow(unused)]
  #[cold]
  pub const fn capacity(&self) -> usize {
    self.capacity
  }
}

impl<K, V> Drop for CacheNode<K, V> {
  fn drop(&mut self) {
    for ptr in self
      .main
      .drain(..)
      .chain(self.small.drain(..))
      .chain(self.ghost.drain(..))
      .filter(|ptr| matches!(ptr.borrow_unsafe().get_state(), State::Tombstone))
    {
      ptr.drop_unsafe();
    }
    for ptr in self.table.drain() {
      if matches!(
        ptr.borrow_unsafe().get_state(),
        State::Small { .. } | State::Main { .. }
      ) {
        ptr.borrow_mut_unsafe().drop_value();
      }
      ptr.drop_unsafe();
    }
  }
}

pub enum GetOrReserve<'a, K, V, R> {
  Hit(&'a V),
  Reserved(Reserved<K, V, R>),
}

/**
 * Reserved uninitialized value slot in the cache.
 *
 * Cache insertion first reserves capacity and, if necessary, evicts an existing
 * value. The caller may need to process that evicted value before it can build
 * the replacement, so the cache returns a reserved value slot instead of taking
 * `V` immediately. A `Reserved` must be fulfilled exactly once before it is
 * dropped.
 */
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
#[path = "tests/node.rs"]
mod tests;
