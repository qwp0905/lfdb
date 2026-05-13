use std::{
  collections::{BTreeMap, BTreeSet, VecDeque},
  hash::{BuildHasher, RandomState},
  mem::ManuallyDrop,
  sync::{Arc, Mutex},
};

use crossbeam::utils::Backoff;

use super::{CacheShard, GetOrReserve, TempBlock, TempBlockRef};
use crate::{
  disk::Pointer,
  table::TableId,
  utils::{ExclusivePin, ExclusiveToken, SharedToken, ShortenedMutex, ToArc},
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
  inner: CacheShard<Key, BlockId>,
  eviction: BTreeSet<Key>,                  // evicting pointers
  temporary: BTreeMap<Key, Arc<TempBlock>>, // temporary pages without promotion
  allocated: BlockId,
  aborted: VecDeque<(BlockId, Option<Key>)>,
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
      shard.inner.remove(&self.new_pointer, self.new_pointer_hash);
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

pub enum Peeked<'a> {
  Hit(BlockId, SharedToken<'a>),
  Temp(TempBlockRef<SharedToken<'a>>),
  DiskRead(TempBlockRef<ExclusiveToken<'a>>, TempGuard<'a>),
}
pub enum Acquired<'a> {
  Hit(BlockId, SharedToken<'a>),
  Temp(TempBlockRef<SharedToken<'a>>),
  Evicted(EvictionGuard<'a>),
}

pub struct MappingTable {
  shards: Box<[Mutex<Shard>]>,
  offsets: Box<[BlockId]>,
  hasher: RandomState,
  capacity: usize,
}
impl MappingTable {
  pub fn new(shard_count: usize, capacity: usize) -> Self {
    let cap_per_shard = capacity / shard_count;
    let mut shards = Vec::with_capacity(shard_count);
    let mut offset = Vec::with_capacity(shard_count);
    for i in 0..shard_count {
      let shard = Shard {
        inner: CacheShard::new(cap_per_shard),
        eviction: BTreeSet::new(),
        temporary: BTreeMap::new(),
        allocated: 0,
        aborted: VecDeque::new(),
      };
      shards.push(Mutex::new(shard));
      offset.push(i * cap_per_shard);
    }

    Self {
      shards: shards.into_boxed_slice(),
      offsets: offset.into_boxed_slice(),
      hasher: RandomState::new(),
      capacity: cap_per_shard,
    }
  }
  fn get_shard(&self, key: Key) -> (u64, &Mutex<Shard>, usize) {
    let h = self.hasher.hash_one(key);
    // Lemire fast modulo
    let i = (((h & U32_MASK) * self.shards.len() as u64) >> 32) as usize;
    let shard = &self.shards[i];
    let offset = self.offsets[i];
    (h, shard, offset)
  }

  pub fn peek<'a, F>(
    &'a self,
    table_id: TableId,
    pointer: Pointer,
    get_pin: F,
  ) -> Peeked<'a>
  where
    F: Fn(BlockId) -> &'a ExclusivePin,
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

      if let Some(block) = shard.temporary.get(&key) {
        if let Some(block_ref) = TempBlockRef::shared(block) {
          return Peeked::Temp(block_ref);
        }

        drop(shard);
        backoff.snooze();
        continue;
      }

      if let Some(&fid) = shard.inner.peek(&key, hash) {
        if let Some(token) = get_pin(fid).try_shared() {
          return Peeked::Hit(fid, token);
        }

        drop(shard);
        backoff.snooze();
        continue;
      }

      let block = TempBlock::new(pointer).to_arc();
      let block_ref = TempBlockRef::exclusive(&block).unwrap();
      shard.temporary.insert(key, block);
      return Peeked::DiskRead(block_ref, TempGuard::new(s, key));
    }
  }

  /**
   * Acquires access to a page by index, following this order:
   *
   * 1. If the index is being evicted, wait — the block is temporarily inaccessible.
   * 2. If GC has allocated a temp page for this index, return it — the temp page
   *    takes precedence over the shard since it reflects the latest state.
   * 3. If the index is in the cache shard, return a hit.
   * 4. If the shard has an empty slot, allocate a new block without eviction.
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

      if let Some(block) = shard.temporary.get(&key) {
        if let Some(block_ref) = TempBlockRef::shared(block) {
          return Acquired::Temp(block_ref);
        }

        drop(shard);
        backoff.snooze();
        continue;
      }

      let mut reserved = match shard
        .inner
        .get_or_reserve(&key, hash, hasher, |&bid| get_pin(bid).try_exclusive())
      {
        Ok(GetOrReserve::Hit(&bid)) => {
          if let Some(token) = get_pin(bid).try_shared() {
            return Acquired::Hit(bid, token);
          }
          drop(shard);
          backoff.snooze();
          continue;
        }
        Ok(GetOrReserve::Reserved(reserved)) => reserved,
        Err(_) => {
          drop(shard);
          backoff.snooze();
          continue;
        }
      };

      let (evicted, bid, token) = match reserved.take_evicted() {
        Some(evicted) => evicted,
        None => {
          let (bid, evicted) = shard.aborted.pop_front().unwrap_or_else(|| {
            let id = shard.allocated;
            shard.allocated += 1;
            debug_assert!(shard.allocated <= self.capacity, "capacity exceeded");
            (id + offset, None)
          });
          reserved.fulfill(bid);
          let token = get_pin(bid).try_exclusive().unwrap();
          return Acquired::Evicted(EvictionGuard::new(
            evicted, bid, token, &s, key, hash,
          ));
        }
      };

      shard.eviction.insert(evicted);
      reserved.fulfill(bid);
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
      .map(|(i, s)| (s.l().allocated, self.offsets[i]))
  }
}

// Safe because all mutable access to MappingTable (which contains raw pointers)
// is guarded by a Mutex, and all public methods take &self.
unsafe impl Sync for MappingTable {}
unsafe impl Send for MappingTable {}
