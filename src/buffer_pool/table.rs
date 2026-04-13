use std::{
  collections::{BTreeMap, BTreeSet},
  hash::{BuildHasher, RandomState},
  sync::{Arc, Mutex},
};

use crossbeam::utils::Backoff;

use super::{FrameState, LRUShard, TempFrameState};
use crate::{
  disk::{PagePool, Pointer, PAGE_SIZE},
  table::TableId,
  utils::{SpinningWait, ToArc},
};

type Key = (TableId, Pointer);

/**
 * BTreeSet/BTreeMap instead of HashMap: hashbrown (swisstable) does not
 * shrink its allocation on removal, which is problematic for long-running
 * servers. Since the number of entries here is expected to be very small
 * at any given time, the performance difference is negligible.
 */
pub struct Shard {
  lru: LRUShard<Key, Arc<FrameState>>,
  eviction: BTreeSet<Key>, // evicting pointers
  temporary: BTreeMap<Key, Arc<TempFrameState>>, // temporary pages for gc without promote
}
impl Shard {
  pub fn remove_temp(&mut self, table_id: TableId, ptr: Pointer) {
    self.temporary.remove(&(table_id, ptr));
  }
}

/**
 * Holds exclusive control over a frame during eviction.
 *
 * Two pointers are blocked simultaneously while this guard is alive:
 * - The old pointer is added to the eviction set, preventing other threads
 *   from reading a dirty page that has not yet been written to disk.
 * - The new pointer maps to a frame in EVICTION_BIT state, preventing access
 *   before the page has been loaded from disk.
 *
 * Call commit() to finalize the eviction and unblock both pointers.
 * If dropped without commit (e.g. on IO failure), the old mapping is
 * restored and the frame is returned to an evictable state.
 */
pub struct EvictionGuard<'a> {
  evicted: Option<(Key, u64)>,
  state: Arc<FrameState>,
  guard: &'a Mutex<Shard>,
  hasher: &'a RandomState,
  new_pointer: Key,
  new_pointer_hash: u64,
  committed: bool,
}

impl<'a> EvictionGuard<'a> {
  fn new(
    evicted: Option<(Key, u64)>,
    state: Arc<FrameState>,
    guard: &'a Mutex<Shard>,
    hasher: &'a RandomState,
    new_pointer: Key,
    new_pointer_hash: u64,
  ) -> Self {
    Self {
      evicted,
      state,
      guard,
      hasher,
      new_pointer,
      new_pointer_hash,
      committed: false,
    }
  }

  pub fn get_frame_id(&self) -> usize {
    self.state.get_frame_id()
  }
  pub fn get_state(&self) -> Arc<FrameState> {
    self.state.clone()
  }
  pub fn get_evicted_pointer(&self) -> Option<Pointer> {
    self.evicted.as_ref().map(|(i, _)| i.1)
  }
  pub fn commit(&mut self) {
    self.committed = true;
  }
}
impl<'a> Drop for EvictionGuard<'a> {
  fn drop(&mut self) {
    if self.committed {
      if let Some((i, _)) = self.evicted {
        self.guard.spin_lock().eviction.remove(&i);
      }
      // This guard now owns the frame (pin count = 1).
      self.state.completion_evict(1);
      return;
    }

    // rollback
    {
      let mut shard = self.guard.spin_lock();
      if let Some((i, h)) = self.evicted {
        shard.eviction.remove(&i);
        shard.lru.insert(i, self.state.clone(), h, self.hasher);
      }
      shard
        .lru
        .remove(&self.new_pointer, self.new_pointer_hash, self.hasher);
    }
    // No ownership claimed — frame is immediately available for eviction.
    self.state.completion_evict(0);
  }
}

pub struct TempGuard<'a> {
  state: Arc<TempFrameState>,
  shard: &'a Mutex<Shard>,
}
impl<'a> TempGuard<'a> {
  fn new(state: Arc<TempFrameState>, shard: &'a Mutex<Shard>) -> Self {
    Self { state, shard }
  }
  pub fn take(self) -> (Arc<TempFrameState>, &'a Mutex<Shard>) {
    (self.state, self.shard)
  }
}

pub enum Acquired<'a> {
  Temp(TempGuard<'a>),
  Hit(Arc<FrameState>),
  Evicted(EvictionGuard<'a>),
}
pub enum Peeked<'a> {
  Hit(Arc<FrameState>),
  Temp(TempGuard<'a>),
  DiskRead(TempGuard<'a>),
}

pub struct LRUTable {
  shards: Vec<Mutex<Shard>>,
  offset: Vec<usize>,
  hasher: RandomState,
  page_pool: Arc<PagePool<PAGE_SIZE>>,
}
impl LRUTable {
  pub fn new(
    page_pool: Arc<PagePool<PAGE_SIZE>>,
    shard_count: usize,
    capacity: usize,
  ) -> Self {
    let cap_per_shard = capacity / shard_count;
    let mut shards = Vec::with_capacity(shard_count);
    let mut offset = Vec::with_capacity(shard_count);
    for i in 0..shard_count {
      let shard = Shard {
        lru: LRUShard::new(cap_per_shard),
        eviction: BTreeSet::new(),
        temporary: BTreeMap::new(),
      };
      shards.push(Mutex::new(shard));
      offset.push(i * cap_per_shard);
    }

    Self {
      shards,
      offset,
      hasher: Default::default(),
      page_pool,
    }
  }
  fn get_shard(&self, key: Key) -> (u64, &Mutex<Shard>, usize) {
    let h = self.hasher.hash_one(key);
    let i = h as usize % self.shards.len();
    let shard = &self.shards[i];
    let offset = self.offset[i];
    (h, shard, offset)
  }

  /**
   * Reads a page without promoting it in the LRU (used by GC).
   *
   * 1. If the pointer is being evicted, wait.
   * 2. If a temp page is already allocated for this pointer, return it.
   * 3. If the pointer is in the LRU, return it without promotion.
   * 4. If not found anywhere, allocate a new temp page in EVICTION_BIT state
   *    and return DiskRead — the caller is responsible for loading from disk.
   */
  pub fn peek_or_temp<'a>(&'a self, table_id: TableId, pointer: Pointer) -> Peeked<'a> {
    let key = (table_id, pointer);
    let (hash, s, _) = self.get_shard(key);
    let backoff = Backoff::new();

    loop {
      let mut shard = s.spin_lock();
      if shard.eviction.contains(&key) {
        drop(shard);
        backoff.snooze();
        continue;
      }

      if let Some(state) = shard.temporary.get(&key) {
        if state.try_pin() {
          return Peeked::Temp(TempGuard::new(state.clone(), s));
        }

        drop(shard);
        backoff.snooze();
        continue;
      }

      if let Some(state) = shard.lru.peek(&key, hash) {
        if state.try_pin() {
          return Peeked::Hit(state.clone());
        }

        drop(shard);
        backoff.snooze();
        continue;
      }

      let state = TempFrameState::new(self.page_pool.acquire()).to_arc();
      shard.temporary.insert(key, state.clone());
      return Peeked::DiskRead(TempGuard::new(state, s));
    }
  }

  /**
   * Acquires access to a page by index, following this order:
   *
   * 1. If the index is being evicted, wait — the frame is temporarily inaccessible.
   * 2. If GC has allocated a temp page for this index, return it — the temp page
   *    takes precedence over the LRU since it reflects the latest state.
   * 3. If the index is in the LRU cache, return a hit.
   * 4. If the LRU has an empty slot, allocate a new frame without eviction.
   * 5. Otherwise, evict the LRU tail and return an EvictionGuard for the caller
   *    to perform the necessary IO.
   *
   * The shard lock is dropped before retrying CAS operations (try_pin, try_evict)
   * to minimize lock contention — holding the lock during CAS would block all
   * other threads on this shard unnecessarily.
   */
  pub fn acquire<'a>(&'a self, table_id: TableId, pointer: Pointer) -> Acquired<'a> {
    let key = (table_id, pointer);
    let (hash, s, offset) = self.get_shard(key);
    let hasher = &self.hasher;
    let backoff = Backoff::new();

    loop {
      let mut shard = s.spin_lock();
      if shard.eviction.contains(&key) {
        drop(shard);
        backoff.snooze();
        continue;
      }

      if let Some(state) = shard.temporary.get(&key) {
        if state.try_pin() {
          return Acquired::Temp(TempGuard::new(state.clone(), s));
        }

        drop(shard);
        backoff.snooze();
        continue;
      }

      if let Some(state) = shard.lru.get(&key, hash, hasher) {
        if state.try_pin() {
          return Acquired::Hit(state.clone());
        }

        drop(shard);
        backoff.snooze();
        continue;
      }

      // Each shard owns a dedicated range of frame IDs [offset, offset + cap_per_shard).
      // This ensures shards never access the same frame, eliminating contention
      // between shards entirely.
      if !shard.lru.is_full() {
        let frame_id = shard.lru.len() + offset;
        let state = FrameState::new(frame_id).to_arc();
        shard.lru.insert(key, state.clone(), hash, hasher);
        return Acquired::Evicted(EvictionGuard::new(None, state, &s, hasher, key, hash));
      }

      let ((evicted, state), evicted_hash) = shard.lru.evict(&self.hasher).unwrap();
      if !state.try_evict() {
        shard.lru.insert(evicted, state, evicted_hash, hasher);
        drop(shard);
        backoff.snooze();
        continue;
      }

      shard.eviction.insert(evicted);
      shard.lru.insert(key, state.clone(), hash, hasher);
      return Acquired::Evicted(EvictionGuard::new(
        Some((evicted, evicted_hash)),
        state,
        &s,
        hasher,
        key,
        hash,
      ));
    }
  }

  pub fn len_per_shard(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
    self
      .shards
      .iter()
      .enumerate()
      .map(|(i, s)| (s.spin_lock().lru.len(), self.offset[i]))
  }
}

// Safe because all mutable access to LRUShard (which contains raw pointers)
// is guarded by a Mutex, and all public methods take &self.
unsafe impl Sync for LRUTable {}
unsafe impl Send for LRUTable {}

#[cfg(test)]
#[path = "tests/table.rs"]
mod tests;
