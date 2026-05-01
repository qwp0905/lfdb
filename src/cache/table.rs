use std::{
  collections::{BTreeMap, BTreeSet},
  hash::{BuildHasher, RandomState},
  mem::ManuallyDrop,
  sync::{Arc, Mutex},
};

use crossbeam::utils::Backoff;

use super::{LRUShard, TempBlockRef, TempBlockState};
use crate::{
  disk::Pointer,
  table::TableId,
  utils::{ExclusivePin, ExclusiveToken, SharedToken, ShortenedMutex, ToArc},
};

type Key = (TableId, Pointer);

pub type BlockId = usize;

/**
 * BTreeSet/BTreeMap instead of HashMap: hashbrown (swisstable) does not
 * shrink its allocation on removal, which is problematic for long-running
 * servers. Since the number of entries here is expected to be very small
 * at any given time, the performance difference is negligible.
 */
struct Shard {
  lru: LRUShard<Key, BlockId>,
  eviction: BTreeSet<Key>, // evicting pointers
  temporary: BTreeMap<Key, Arc<TempBlockState>>, // temporary pages for gc without promote
}

/**
 * Holds exclusive control over a block during eviction.
 *
 * Two pointers are blocked simultaneously while this guard is alive:
 * - The old pointer is added to the eviction set, preventing other threads
 *   from reading a dirty page that has not yet been written to disk.
 * - The new pointer maps to a block in EVICTION_BIT state, preventing access
 *   before the page has been loaded from disk.
 *
 * Call commit() to finalize the eviction and unblock both pointers.
 * If dropped without commit (e.g. on IO failure), the old mapping is
 * restored and the block is returned to an evictable state.
 */
pub struct EvictionGuard<'a> {
  evicted: Option<(Key, u64)>,
  block_id: BlockId,
  token: ManuallyDrop<ExclusiveToken<'a>>,
  guard: &'a Mutex<Shard>,
  hasher: &'a RandomState,
  new_pointer: Key,
  new_pointer_hash: u64,
  committed: bool,
}

impl<'a> EvictionGuard<'a> {
  const fn new(
    evicted: Option<(Key, u64)>,
    block_id: usize,
    token: ExclusiveToken<'a>,
    guard: &'a Mutex<Shard>,
    hasher: &'a RandomState,
    new_pointer: Key,
    new_pointer_hash: u64,
  ) -> Self {
    Self {
      evicted,
      block_id,
      token: ManuallyDrop::new(token),
      guard,
      hasher,
      new_pointer,
      new_pointer_hash,
      committed: false,
    }
  }

  pub const fn get_block_id(&self) -> usize {
    self.block_id
  }
  pub const fn is_evicted(&self) -> bool {
    self.evicted.is_some()
  }
  pub fn commit(mut self) -> SharedToken<'a> {
    self.committed = true;
    unsafe { ManuallyDrop::take(&mut self.token) }.downgrade()
  }
}
impl<'a> Drop for EvictionGuard<'a> {
  fn drop(&mut self) {
    if self.committed {
      if let Some((i, _)) = self.evicted {
        self.guard.l().eviction.remove(&i);
      }
      return;
    }

    // rollback
    {
      let mut shard = self.guard.l();
      if let Some((i, h)) = self.evicted {
        shard.eviction.remove(&i);
        shard.lru.insert(i, self.block_id, h, self.hasher);
      }
      shard
        .lru
        .remove(&self.new_pointer, self.new_pointer_hash, self.hasher);
    }
    // No ownership claimed — block is immediately available for eviction.
    unsafe { ManuallyDrop::drop(&mut self.token) };
  }
}

pub struct TempGuard<'a> {
  shard: &'a Mutex<Shard>,
  key: Key,
}
impl<'a> TempGuard<'a> {
  #[inline]
  const fn new(shard: &'a Mutex<Shard>, key: Key) -> Self {
    Self { shard, key }
  }
}
impl<'a> Drop for TempGuard<'a> {
  #[inline]
  fn drop(&mut self) {
    self.shard.l().temporary.remove(&self.key);
  }
}

pub enum Acquired<'a> {
  Hit(BlockId, SharedToken<'a>),
  Temp(TempBlockRef<'a, SharedToken<'a>>),
  Evicted(EvictionGuard<'a>),
}
pub enum Peeked<'a> {
  Hit(BlockId, SharedToken<'a>),
  Temp(TempBlockRef<'a, SharedToken<'a>>),
  DiskRead(TempBlockRef<'a, ExclusiveToken<'a>>, TempGuard<'a>),
}

pub struct LRUTable {
  shards: Vec<Mutex<Shard>>,
  offset: Vec<BlockId>,
  hasher: RandomState,
}
impl LRUTable {
  pub fn new(shard_count: usize, capacity: usize) -> Self {
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
      hasher: RandomState::new(),
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
   * 4. If not found anywhere, allocate a new temp page exclusive state
   *    and return DiskRead — the caller is responsible for loading from disk.
   */
  pub fn peek_or_temp<'a, F>(
    &'a self,
    table_id: TableId,
    pointer: Pointer,
    get_pin: F,
  ) -> Peeked<'a>
  where
    F: Fn(usize) -> &'a ExclusivePin,
  {
    let key = (table_id, pointer);
    let (hash, s, _) = self.get_shard(key);
    let backoff = Backoff::new();

    loop {
      let mut shard = s.l();
      if shard.eviction.contains(&key) {
        drop(shard);
        backoff.snooze();
        continue;
      }

      if let Some(state) = shard.temporary.get(&key) {
        if let Some(state_ref) = TempBlockRef::shared(state) {
          return Peeked::Temp(state_ref);
        }

        drop(shard);
        backoff.snooze();
        continue;
      }

      if let Some(&fid) = shard.lru.peek(&key, hash) {
        if let Some(token) = get_pin(fid).try_shared() {
          return Peeked::Hit(fid, token);
        }

        drop(shard);
        backoff.snooze();
        continue;
      }

      let state = TempBlockState::new().to_arc();
      let state_ref = TempBlockRef::exclusive(&state).unwrap();
      shard.temporary.insert(key, state_ref.get_state());
      return Peeked::DiskRead(state_ref, TempGuard::new(s, key));
    }
  }

  /**
   * Acquires access to a page by index, following this order:
   *
   * 1. If the index is being evicted, wait — the block is temporarily inaccessible.
   * 2. If GC has allocated a temp page for this index, return it — the temp page
   *    takes precedence over the LRU since it reflects the latest state.
   * 3. If the index is in the LRU cache, return a hit.
   * 4. If the LRU has an empty slot, allocate a new block without eviction.
   * 5. Otherwise, evict the LRU tail and return an EvictionGuard for the caller
   *    to perform the necessary IO.
   *
   * The shard lock is dropped before retrying CAS operations (try_pin, try_evict)
   * to minimize lock contention — holding the lock during CAS would block all
   * other threads on this shard unnecessarily.
   */
  pub fn acquire<'a, F>(
    &'a self,
    table_id: TableId,
    pointer: Pointer,
    get_pin: F,
  ) -> Acquired<'a>
  where
    F: Fn(BlockId) -> &'a ExclusivePin,
  {
    let key = (table_id, pointer);
    let (hash, s, offset) = self.get_shard(key);
    let hasher = &self.hasher;
    let backoff = Backoff::new();

    loop {
      let mut shard = s.l();
      if shard.eviction.contains(&key) {
        drop(shard);
        backoff.snooze();
        continue;
      }

      if let Some(state) = shard.temporary.get(&key) {
        if let Some(state_ref) = TempBlockRef::shared(state) {
          return Acquired::Temp(state_ref);
        }

        drop(shard);
        backoff.snooze();
        continue;
      }

      if let Some(&fid) = shard.lru.get(&key, hash, hasher) {
        if let Some(token) = get_pin(fid).try_shared() {
          return Acquired::Hit(fid, token);
        }

        drop(shard);
        backoff.snooze();
        continue;
      }

      // Each shard owns a dedicated range of block IDs [offset, offset + cap_per_shard).
      // This ensures shards never access the same block, eliminating contention
      // between shards entirely.
      if !shard.lru.is_full() {
        let fid = shard.lru.len() + offset;
        let token = get_pin(fid).try_exclusive().unwrap();
        shard.lru.insert(key, fid, hash, hasher);
        return Acquired::Evicted(EvictionGuard::new(
          None, fid, token, &s, hasher, key, hash,
        ));
      }

      let ((evicted, fid), evicted_hash) = shard.lru.evict(&self.hasher).unwrap();
      let token = match get_pin(fid).try_exclusive() {
        Some(t) => t,
        None => {
          shard.lru.insert(evicted, fid, evicted_hash, hasher);
          drop(shard);
          backoff.snooze();
          continue;
        }
      };

      shard.eviction.insert(evicted);
      shard.lru.insert(key, fid, hash, hasher);
      return Acquired::Evicted(EvictionGuard::new(
        Some((evicted, evicted_hash)),
        fid,
        token,
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
      .map(|(i, s)| (s.l().lru.len(), self.offset[i]))
  }
}

// Safe because all mutable access to LRUShard (which contains raw pointers)
// is guarded by a Mutex, and all public methods take &self.
unsafe impl Sync for LRUTable {}
unsafe impl Send for LRUTable {}
