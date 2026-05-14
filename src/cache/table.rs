use std::{
  collections::{BTreeSet, VecDeque},
  hash::{BuildHasher, RandomState},
  mem::ManuallyDrop,
  sync::Mutex,
};

use crossbeam::utils::Backoff;

use super::LRUShard;
use crate::{
  disk::Pointer,
  table::TableId,
  utils::{ExclusivePin, ExclusiveToken, SharedToken, ShortenedMutex},
};

type Key = (TableId, Pointer);

pub type BlockId = usize;

const U32_MASK: u64 = u32::MAX as u64;

/**
 * BTreeSet/BTreeMap instead of HashMap: hashbrown (swisstable) does not
 * shrink its allocation on removal, which is problematic for long-running
 * servers. Since the number of entries here is expected to be very small
 * at any given time, the performance difference is negligible.
 */
struct Shard {
  lru: LRUShard<Key, BlockId>,
  allocated: BlockId,
  aborted: VecDeque<(BlockId, Option<Key>)>,
  eviction: BTreeSet<Key>, // evicting pointers
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
  evicted: Option<Key>,
  block_id: BlockId,
  token: ManuallyDrop<ExclusiveToken<'a>>,
  guard: &'a Mutex<Shard>,
  new_pointer: Key,
  new_pointer_hash: u64,
  committed: bool,
}

impl<'a> EvictionGuard<'a> {
  const fn new(
    evicted: Option<Key>,
    block_id: usize,
    token: ExclusiveToken<'a>,
    guard: &'a Mutex<Shard>,
    new_pointer: Key,
    new_pointer_hash: u64,
  ) -> Self {
    Self {
      evicted,
      block_id,
      token: ManuallyDrop::new(token),
      guard,
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
      if let Some(i) = self.evicted {
        self.guard.l().eviction.remove(&i);
      }
      return;
    }

    // rollback
    {
      let mut shard = self.guard.l();
      if let Some(i) = self.evicted {
        shard.eviction.remove(&i);
        shard.aborted.push_back((self.block_id, Some(i)));
      } else {
        shard.aborted.push_back((self.block_id, None));
      }
      shard.lru.remove(&self.new_pointer, self.new_pointer_hash);
    }
    // No ownership claimed — block is immediately available for eviction.
    unsafe { ManuallyDrop::drop(&mut self.token) };
  }
}

pub enum Acquired<'a> {
  Hit(BlockId, SharedToken<'a>),
  Evicted(EvictionGuard<'a>),
}

pub struct LRUTable {
  shards: Box<[Mutex<Shard>]>,
  offset: Box<[BlockId]>,
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
        allocated: 0,
        aborted: VecDeque::new(),
        eviction: BTreeSet::new(),
      };
      shards.push(Mutex::new(shard));
      offset.push(i * cap_per_shard);
    }

    Self {
      shards: shards.into_boxed_slice(),
      offset: offset.into_boxed_slice(),
      hasher: RandomState::new(),
    }
  }
  fn get_shard(&self, key: Key) -> (u64, &Mutex<Shard>, usize) {
    let h = self.hasher.hash_one(key);
    // Lemire fast modulo
    let i = (((h & U32_MASK) * self.shards.len() as u64) >> 32) as usize;
    let shard = &self.shards[i];
    let offset = self.offset[i];
    (h, shard, offset)
  }

  /**
   * Acquires access to a page by index, following this order:
   *
   * 1. If the index is being evicted, wait — the block is temporarily inaccessible.
   * 2. If the index is in the LRU cache, return a hit.
   * 3. If the LRU has an empty slot, allocate a new block without eviction.
   * 4. Otherwise, evict the LRU tail and return an EvictionGuard for the caller
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
    let try_evict = |&bid: &BlockId| get_pin(bid).try_exclusive();

    loop {
      let mut shard = s.l();
      if shard.eviction.contains(&key) {
        drop(shard);
        backoff.snooze();
        continue;
      }

      if let Some(&fid) = shard.lru.get(&key, hash) {
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
        let (bid, evicted) = shard.aborted.pop_front().unwrap_or_else(|| {
          let id = shard.allocated;
          shard.allocated += 1;
          (id + offset, None)
        });
        let token = try_evict(&bid).unwrap();
        shard.lru.insert(key, bid, hash, hasher);
        return Acquired::Evicted(EvictionGuard::new(evicted, bid, token, &s, key, hash));
      }

      let (evicted, bid, token) = match shard.lru.evict_if(&self.hasher, &try_evict) {
        Some(v) => v,
        None => {
          drop(shard);
          backoff.snooze();
          continue;
        }
      };

      shard.eviction.insert(evicted);
      shard.lru.insert(key, bid, hash, hasher);
      return Acquired::Evicted(EvictionGuard::new(
        Some(evicted),
        bid,
        token,
        &s,
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
