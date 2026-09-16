use std::{
  borrow::Borrow,
  hash::{BuildHasher, Hash},
  mem::replace,
};

use hashbrown::Equivalent;

use super::{ShrinkQueue, ShrinkTable};

enum State<K, V> {
  Small {
    key: K,
    value: V,
    freq: u8,
  },
  Main {
    key: K,
    value: V,
    freq: u8,
  },
  Ghost {
    key: K,
  },

  /**
   * Removed from the lookup table and no longer owns a live value, but the raw
   * pointer may still be present in one of the FIFO queues. It is freed lazily
   * when that queue entry reaches the front.
   */
  Vacant,
}

struct Entry<K, V> {
  epoch: EntryEpoch,
  state: State<K, V>,
}

type EntryId = usize;
type EntryEpoch = u64;

impl<K, V> Entry<K, V> {
  const fn vacant() -> Self {
    Self {
      epoch: 0,
      state: State::Vacant,
    }
  }

  const fn get_state(&self) -> &State<K, V> {
    &self.state
  }
  const fn get_state_mut(&mut self) -> &mut State<K, V> {
    &mut self.state
  }
  const fn take_state(&mut self) -> State<K, V> {
    replace(&mut self.state, State::Vacant)
  }
  fn set_state(&mut self, state: State<K, V>) {
    self.state = state;
  }
  const fn get_epoch(&self) -> EntryEpoch {
    self.epoch
  }
  const fn inc_epoch(&mut self) -> EntryEpoch {
    self.epoch += 1;
    self.epoch
  }
}

const fn entry_eq<'a>(id: EntryId) -> impl Fn(&EntryId) -> bool + 'a {
  move |&i| i == id
}

struct CacheEntries<K, V> {
  slots: Box<[Entry<K, V>]>,
  reusable: ShrinkQueue<EntryId>,
}
impl<K, V> CacheEntries<K, V> {
  fn new(capacity: usize) -> Self {
    let mut slots = Vec::with_capacity(capacity);
    let mut reusable = ShrinkQueue::with_capacity(capacity);
    for i in 0..capacity {
      reusable.push(i);
      slots.push(Entry::vacant());
    }
    let slots = slots.into_boxed_slice();
    Self { slots, reusable }
  }

  const fn equivalent<'a, Q: ?Sized + Equivalent<K>>(
    &'a self,
    k: &'a Q,
  ) -> impl Fn(&EntryId) -> bool + 'a {
    |&id| match self.get(id).get_state() {
      State::Small { key, .. } => k.equivalent(key),
      State::Main { key, .. } => k.equivalent(key),
      State::Ghost { key } => k.equivalent(key),
      State::Vacant => unreachable!(),
    }
  }
  fn make_hasher<'a, S>(&'a self, build_hasher: &'a S) -> impl Fn(&EntryId) -> u64 + 'a
  where
    K: Hash,
    S: BuildHasher,
  {
    move |&id| match self.get(id).get_state() {
      State::Small { key, .. } => build_hasher.hash_one(key),
      State::Main { key, .. } => build_hasher.hash_one(key),
      State::Ghost { key } => build_hasher.hash_one(key),
      State::Vacant => unreachable!(),
    }
  }

  fn get(&self, id: EntryId) -> &Entry<K, V> {
    &self.slots[id]
  }
  fn get_mut(&mut self, id: EntryId) -> &mut Entry<K, V> {
    &mut self.slots[id]
  }

  fn reuse(&mut self, id: EntryId) {
    self.reusable.push(id);
  }

  fn get_next_id(&mut self) -> Option<EntryId> {
    self.reusable.pop()
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
  entries: CacheEntries<K, V>,
  table: ShrinkTable<EntryId>,
  small: ShrinkQueue<(EntryId, EntryEpoch)>,
  main: ShrinkQueue<(EntryId, EntryEpoch)>,
  ghost: ShrinkQueue<(EntryId, EntryEpoch)>,
  capacity: usize,
  small_cap: usize,
  small_count: usize,
  main_count: usize,
  ghost_cap: usize,
}
impl<K, V> CacheNode<K, V> {
  pub fn new(capacity: usize) -> Self {
    let small_cap = capacity / 10;
    let main_cap = capacity - small_cap;
    let ghost_cap = main_cap;
    Self {
      entries: CacheEntries::new(small_cap + main_cap + ghost_cap),
      table: ShrinkTable::new(),
      small: ShrinkQueue::new(),
      main: ShrinkQueue::new(),
      ghost: ShrinkQueue::new(),
      capacity,
      small_cap,
      small_count: 0,
      main_count: 0,
      ghost_cap,
    }
  }

  const fn len(&self) -> usize {
    self.main_count + self.small_count
  }

  #[allow(unused)]
  #[cold]
  pub const fn capacity(&self) -> usize {
    self.capacity
  }
}
impl<K, V> CacheNode<K, V>
where
  K: Eq + Hash,
{
  pub fn evict_one<S, R, F>(
    &mut self,
    build_hasher: &S,
    try_evict: F,
  ) -> std::result::Result<Evicted<K, V, R>, ()>
  where
    K: Clone,
    S: BuildHasher,
    F: Fn(&V) -> Option<R>,
  {
    let evicted = self.evict_internal(build_hasher, try_evict)?;
    Ok(Evicted::new(evicted, EvictedTo::Small))
  }

  pub fn insert_to<S>(
    &mut self,
    key: K,
    hash: u64,
    value: V,
    build_hasher: &S,
    toward: EvictedTo,
  ) where
    S: BuildHasher,
  {
    debug_assert!({
      let eq = self.entries.equivalent(&key);
      self.table.find(hash, eq).is_none()
    });

    let id = self.entries.get_next_id().unwrap();
    let entry = self.entries.get_mut(id);
    let epoch = entry.inc_epoch();
    let state = match toward {
      EvictedTo::Main => {
        self.main.push((id, epoch));
        self.main_count += 1;
        State::Main {
          key,
          value,
          freq: 0,
        }
      }
      EvictedTo::Small => {
        self.small.push((id, epoch));
        self.small_count += 1;
        State::Small {
          key,
          value,
          freq: 0,
        }
      }
    };

    entry.set_state(state);
    self
      .table
      .insert_unique(hash, id, self.entries.make_hasher(build_hasher));
  }

  /**
   * `try_evict` is the external eviction gate. Before inserting a new entry into
   * the raw table, the cache first proves that some live entry can actually be
   * removed; this avoids growing the raw table just because all current victims
   * are temporarily unevictable.
   */
  pub fn get_or_evict<S, R, F>(
    &mut self,
    key: &K,
    hash: u64,
    build_hasher: &S,
    try_evict: F,
  ) -> std::result::Result<GetOrEvicted<'_, K, V, R>, ()>
  where
    K: Clone,
    S: BuildHasher,
    F: Fn(&V) -> Option<R>,
  {
    let Ok(bucket) = self.table.find_entry(hash, self.entries.equivalent(key)) else {
      return self
        .evict_one(build_hasher, try_evict)
        .map(GetOrEvicted::Evicted);
    };

    let id = *bucket.get();
    let entry = self.entries.get_mut(id);
    match entry.get_state_mut() {
      State::Small { freq, .. } | State::Main { freq, .. } => {
        *freq = (*freq + 1).min(MAX_FREQ);
        let (State::Small { value, .. } | State::Main { value, .. }) =
          self.entries.get(id).get_state()
        else {
          unreachable!()
        };
        return Ok(GetOrEvicted::Hit(value));
      }
      State::Ghost { .. } => {
        bucket.remove();
        entry.set_state(State::Vacant);
        self.entries.reuse(id);
      }
      State::Vacant => unreachable!(),
    };

    let evicted = self.evict_internal(build_hasher, &try_evict)?;
    Ok(GetOrEvicted::Evicted(Evicted::new(
      evicted,
      EvictedTo::Main,
    )))
  }

  fn evict_internal<S, R, F>(
    &mut self,
    build_hasher: &S,
    try_evict: F,
  ) -> std::result::Result<Option<(K, V, R, u64)>, ()>
  where
    K: Clone,
    S: BuildHasher,
    F: Fn(&V) -> Option<R>,
  {
    while self.len() >= self.capacity {
      if self.small_count > self.small_cap {
        return self.evict_small(build_hasher, &try_evict).map(Some);
      }
      if let Some(v) = self.evict_main(build_hasher, &try_evict)? {
        return Ok(Some(v));
      }
    }
    Ok(None)
  }

  fn evict_small<S, R, F>(
    &mut self,
    build_hasher: &S,
    try_evict: &F,
  ) -> std::result::Result<(K, V, R, u64), ()>
  where
    K: Clone,
    S: BuildHasher,
    F: Fn(&V) -> Option<R>,
  {
    loop {
      let (id, epoch) = self.small.pop().unwrap();
      let entry = self.entries.get(id);
      if entry.get_epoch() != epoch {
        continue;
      }

      match entry.get_state() {
        State::Small { freq, key, value } if *freq > 1 => {
          let Ok(evicted) = self.evict_main(build_hasher, try_evict) else {
            self.small.push((id, epoch));
            return Err(());
          };

          let entry = self.entries.get_mut(id);
          let State::Small { key, value, .. } = entry.take_state() else {
            unreachable!()
          };
          entry.set_state(State::Main {
            key,
            value,
            freq: 0,
          });

          self.main.push((id, epoch));
          self.small_count -= 1;
          self.main_count += 1;
          if let Some(v) = evicted {
            return Ok(v);
          }
        }
        State::Small { value, .. } => {
          let Some(reserved) = try_evict(value) else {
            self.small.push((id, epoch));
            return Err(());
          };

          self.evict_ghost(build_hasher);

          let entry = self.entries.get_mut(id);
          let State::Small { key, value, .. } = entry.take_state() else {
            unreachable!()
          };
          entry.set_state(State::Ghost { key: key.clone() });

          self.ghost.push((id, epoch));
          self.small_count -= 1;
          let hash = build_hasher.hash_one(&key);
          return Ok((key, value, reserved, hash));
        }
        State::Vacant => {}
        State::Main { .. } | State::Ghost { .. } => unreachable!(),
      };
    }
  }

  fn evict_main<S, R, F>(
    &mut self,
    build_hasher: &S,
    try_evict: &F,
  ) -> std::result::Result<Option<(K, V, R, u64)>, ()>
  where
    S: BuildHasher,
    F: Fn(&V) -> Option<R>,
  {
    while let Some((id, epoch)) = self.main.pop() {
      let entry = self.entries.get_mut(id);
      if entry.get_epoch() != epoch {
        continue;
      }

      match entry.get_state_mut() {
        State::Main { freq, .. } if *freq > 0 => {
          *freq -= 1;
          self.main.push((id, epoch));
          continue;
        }
        State::Main { value, .. } => {
          let Some(reserved) = try_evict(value) else {
            self.main.push((id, epoch));
            return Err(());
          };
          let hasher = self.entries.make_hasher(build_hasher);
          let hash = hasher(&id);
          self
            .table
            .remove_and_shrink(hash, entry_eq(id), hasher)
            .unwrap_or_else(|| unreachable!());

          let State::Main { key, value, .. } = self.entries.get_mut(id).take_state()
          else {
            unreachable!()
          };
          self.entries.reuse(id);
          self.main_count -= 1;
          return Ok(Some((key, value, reserved, hash)));
        }
        State::Small { .. } | State::Ghost { .. } => unreachable!(),
        State::Vacant => {}
      };
    }

    Ok(None)
  }

  fn evict_ghost<S>(&mut self, build_hasher: &S)
  where
    S: BuildHasher,
  {
    // Ghost entries stay in the lookup table but no longer count as live cache
    // values. Their count is `table.len() - len()`, so no separate ghost counter
    // is needed.
    while self.table.len() - self.len() >= self.ghost_cap {
      let (id, epoch) = self.ghost.pop().unwrap_or_else(|| unreachable!());
      let entry = self.entries.get(id);
      if entry.get_epoch() != epoch {
        continue;
      }
      match entry.get_state() {
        State::Main { .. } | State::Small { .. } => unreachable!(),
        State::Ghost { key } => {
          let hash = build_hasher.hash_one(key);
          let hasher = self.entries.make_hasher(build_hasher);
          let eq = entry_eq(id);
          self
            .table
            .remove_and_shrink(hash, eq, hasher)
            .unwrap_or_else(|| unreachable!());

          self.entries.get_mut(id).set_state(State::Vacant);
          self.entries.reuse(id);
        }
        State::Vacant => {}
      }
    }
  }

  pub fn remove<Q, S>(&mut self, key: &Q, hash: u64, build_hasher: &S) -> Option<V>
  where
    K: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
    S: BuildHasher,
  {
    let eq = self.entries.equivalent(key);
    let hasher = self.entries.make_hasher(build_hasher);
    let id = self.table.remove_and_shrink(hash, eq, hasher)?;
    self.entries.reuse(id);

    let entry = self.entries.get_mut(id);
    match entry.take_state() {
      State::Small { value, .. } => {
        self.small_count -= 1;
        Some(value)
      }
      State::Main { value, .. } => {
        self.main_count -= 1;
        Some(value)
      }
      State::Ghost { .. } => None,
      State::Vacant => unreachable!(),
    }
  }
}

pub enum GetOrEvicted<'a, K, V, R> {
  Hit(&'a V),
  Evicted(Evicted<K, V, R>),
}

pub struct Evicted<K, V, R> {
  evicted: Option<(K, V, R, u64)>,
  toward: EvictedTo,
}
impl<K, V, R> Evicted<K, V, R> {
  const fn new(evicted: Option<(K, V, R, u64)>, toward: EvictedTo) -> Self {
    Self { evicted, toward }
  }
  pub const fn take_evicted(&mut self) -> Option<(K, V, R, u64)> {
    self.evicted.take()
  }
  pub const fn toward(&self) -> EvictedTo {
    self.toward
  }
}

#[derive(Clone, Copy)]
pub enum EvictedTo {
  Main,
  Small,
}

#[cfg(test)]
#[path = "tests/node.rs"]
mod tests;
