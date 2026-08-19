use std::{
  cell::OnceCell,
  hash::{BuildHasher, RandomState},
  mem::ManuallyDrop,
  sync::{Mutex, MutexGuard},
};

use crossbeam::utils::Backoff;

use super::{CacheNode, Evicted, GetOrEvicted, ShrinkSet};
use crate::{
  background::OnceParker,
  disk::Pointer,
  table::TableId,
  utils::{ChunkQueue, ExclusivePin, ExclusiveToken, SBox, SharedToken, ShortenedMutex},
};

type Key = (TableId, Pointer);

pub type BlockId = usize;

const U32_MASK: u64 = u32::MAX as u64;

struct Shard {
  node: CacheNode<Key, BlockId>,
  eviction: ShrinkSet<Key, OnceCell<SBox<OnceParker>>>, // evicting pointers
  allocated: BlockId,

  /**
   * Slots from eviction/install attempts that did not commit. The BlockId can be
   * reused by a later miss. `Some(key)` means the slot still belongs to an old
   * logical key and must go through eviction again before reuse; `None` means it
   * is an uncommitted fresh slot with no live key.
   */
  aborted: ChunkQueue<(BlockId, Option<(Key, u64)>)>,
}

/**
 * Holds exclusive control over a cache slot while a new mapping is installed.
 *
 * The new logical key is reserved in the mapping table but must not be readable
 * until its page has been loaded, so it is treated as in-eviction until commit.
 * If an old key was evicted, the guard also owns the old slot's exclusive pin,
 * preventing readers or another eviction from using that slot during the
 * transition.
 *
 * Call `commit` after the caller has populated the cache slot for the new key.
 * Dropping without commit rolls the mapping back and returns the slot to the
 * aborted-slot queue.
 */
pub struct EvictionGuard<'a> {
  evicted: Option<(Key, u64)>,
  block_id: BlockId,
  token: ManuallyDrop<ExclusiveToken<'a>>,
  shard: &'a Mutex<Shard>,
  new_pointer: Key,
  new_pointer_hash: u64,
  committed: bool,
  hasher: &'a RandomState,
}

impl<'a> EvictionGuard<'a> {
  const fn new(
    evicted: Option<(Key, u64)>,
    block_id: usize,
    token: ExclusiveToken<'a>,
    shard: &'a Mutex<Shard>,
    new_pointer: Key,
    new_pointer_hash: u64,
    hasher: &'a RandomState,
  ) -> Self {
    Self {
      evicted,
      block_id,
      token: ManuallyDrop::new(token),
      shard,
      new_pointer,
      new_pointer_hash,
      committed: false,
      hasher,
    }
  }

  pub const fn get_block_id(&self) -> usize {
    self.block_id
  }
  pub const fn is_evicted(&self) -> bool {
    self.evicted.is_some()
  }
  /**
   * Committing means the cache slot has been populated for the new key and is
   * now readable. Downgrade the exclusive slot ownership to shared ownership so
   * the caller can continue with read access while other readers are allowed in.
   */
  pub fn commit(mut self) -> SharedToken<'a> {
    self.committed = true;
    unsafe { ManuallyDrop::take(&mut self.token) }.downgrade()
  }
}
impl<'a> Drop for EvictionGuard<'a> {
  fn drop(&mut self) {
    let mut guard = self.shard.l();
    let parker = self
      .evicted
      .map(|(k, h)| guard.eviction.remove(h, &k, self.hasher))
      .map(|p| p.unwrap_or_else(|| unreachable!()))
      .and_then(|p| p.into_inner());

    if !self.committed {
      // rollback
      guard.aborted.push((self.block_id, self.evicted));
      guard
        .node
        .remove(&self.new_pointer, self.new_pointer_hash, self.hasher)
        .unwrap_or_else(|| unreachable!());
      // No ownership claimed — block is immediately available for eviction.
      unsafe { ManuallyDrop::drop(&mut self.token) };
    }

    drop(guard);
    let Some(parker) = parker else {
      return;
    };
    parker.wake_all();
  }
}

pub enum Acquired<'a> {
  Hit(BlockId, SharedToken<'a>),
  Evicted(EvictionGuard<'a>),
}

/**
 * Sharded logical-block to cache-slot mapping table.
 *
 * This type does not store cached pages themselves. It maps a logical disk
 * address `(TableId, Pointer)` to a cache `BlockId` and drives the eviction
 * protocol when a miss needs a slot. The actual cached blocks and their pins are
 * owned outside this table; callers provide access to those pins through
 * callbacks.
 */
pub struct MappingTable {
  shards: Box<[Mutex<Shard>]>,
  offsets: Box<[BlockId]>,
  hasher: RandomState,
}
impl MappingTable {
  pub fn new(shard_count: usize, capacity: usize) -> Self {
    let cap_per_shard = capacity / shard_count;
    let mut shards = Vec::with_capacity(shard_count);
    let mut offset = Vec::with_capacity(shard_count);
    for i in 0..shard_count {
      let shard = Shard {
        node: CacheNode::new(cap_per_shard),
        eviction: ShrinkSet::new(),
        allocated: 0,
        aborted: ChunkQueue::new(),
      };
      shards.push(Mutex::new(shard));
      offset.push(i * cap_per_shard);
    }

    Self {
      shards: shards.into_boxed_slice(),
      offsets: offset.into_boxed_slice(),
      hasher: RandomState::new(),
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

  /**
   * Reserve a cache slot for a key that is known not to exist.
   *
   * This is used when the caller has created a logically new disk address, such
   * as a newly allocated pointer or a new table. Since the key cannot hit, the
   * method skips lookup semantics and goes directly through reservation/eviction.
   */
  pub fn alloc<'a, F>(
    &'a self,
    table_id: TableId,
    pointer: Pointer,
    get_pin: F,
  ) -> EvictionGuard<'a>
  where
    F: Fn(BlockId) -> &'a ExclusivePin,
  {
    let key = (table_id, pointer);
    let (hash, shard, offset) = self.get_shard(key);
    let hasher = &self.hasher;
    let backoff = Backoff::new();
    let try_evict = |bid: &BlockId| get_pin(*bid).try_exclusive();

    loop {
      let mut guard = shard.l();
      debug_assert!(guard.eviction.get(hash, &key).is_none());
      let Ok(reserved) = guard.node.reserve(hasher, try_evict) else {
        drop(guard);
        backoff.snooze();
        continue;
      };

      if let Some(guard) =
        self.handle_reserved(reserved, key, hash, guard, shard, offset, try_evict)
      {
        return guard;
      };
      backoff.snooze();
    }
  }

  /**
   * Acquire a cache slot for an existing logical block address.
   *
   * Hits return the mapped block id with a shared slot token. Misses reserve a
   * slot and return an eviction guard so the caller can populate the slot before
   * committing the mapping.
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
    let (hash, shard, offset) = self.get_shard(key);
    let hasher = &self.hasher;
    let backoff = Backoff::new();
    let try_evict = |bid: &BlockId| get_pin(*bid).try_exclusive();

    loop {
      let mut guard = shard.l();
      if let Some(parker) = guard.eviction.get(hash, &key) {
        let parker = parker.get_or_init(Default::default).clone();
        drop(guard);
        parker.park();
        continue;
      }

      let Ok(result) = guard.node.get_or_reserve(&key, hash, hasher, try_evict) else {
        drop(guard);
        backoff.snooze();
        continue;
      };
      let evicted = match result {
        GetOrEvicted::Hit(&bid) => {
          if let Some(token) = get_pin(bid).try_shared() {
            return Acquired::Hit(bid, token);
          }
          drop(guard);
          backoff.snooze();
          continue;
        }
        GetOrEvicted::Evicted(evicted) => evicted,
      };

      if let Some(guard) =
        self.handle_reserved(evicted, key, hash, guard, shard, offset, try_evict)
      {
        return Acquired::Evicted(guard);
      };
      backoff.snooze();
    }
  }

  fn handle_reserved<'a, F>(
    &'a self,
    mut evicted: Evicted<Key, BlockId, ExclusiveToken<'a>>,
    key: Key,
    hash: u64,
    mut guard: MutexGuard<'a, Shard>,
    shard: &'a Mutex<Shard>,
    offset: usize,
    try_evict: F,
  ) -> Option<EvictionGuard<'a>>
  where
    F: Fn(&BlockId) -> Option<ExclusiveToken<'a>>,
  {
    let toward = evicted.toward();
    if let Some((evicted, bid, token, evicted_hash)) = evicted.take_evicted() {
      // Reuse the evicted cache slot for the new key. The mapping is reserved now,
      // but the slot may still contain the old page until the caller finishes the
      // eviction/load work, so keep the old key blocked during the transition.
      guard.node.insert_to(&key, hash, bid, &self.hasher, toward);
      guard.eviction.insert_unchecked(
        evicted,
        OnceCell::new(),
        evicted_hash,
        &self.hasher,
      );
      return Some(EvictionGuard::new(
        Some((evicted, evicted_hash)),
        bid,
        token,
        shard,
        key,
        hash,
        &self.hasher,
      ));
    }

    let (bid, evicted) = guard.aborted.pop().unwrap_or_else(|| {
      let id = guard.allocated;
      guard.allocated += 1;
      debug_assert!(
        guard.allocated <= guard.node.capacity(),
        "capacity exceeded"
      );
      (id + offset, None)
    });
    guard.node.insert_to(&key, hash, bid, &self.hasher, toward);

    let Some((evicted, evicted_hash)) = evicted else {
      let token = try_evict(&bid).unwrap();
      return Some(EvictionGuard::new(
        None,
        bid,
        token,
        shard,
        key,
        hash,
        &self.hasher,
      ));
    };

    if let Some(token) = try_evict(&bid) {
      guard.eviction.insert_unchecked(
        evicted,
        OnceCell::new(),
        evicted_hash,
        &self.hasher,
      );
      return Some(EvictionGuard::new(
        Some((evicted, evicted_hash)),
        bid,
        token,
        shard,
        key,
        hash,
        &self.hasher,
      ));
    }

    // It is not certain whether an eviction block in the aborted queue can acquire exclusive rights
    // due to contention with checkpoints or other reads.
    // However, since this occurs very rarely due to reasons such as disk failure, it is fine to proceed with deleting the hash table.
    guard.node.remove(&key, hash, &self.hasher);
    guard.aborted.push((bid, Some((evicted, evicted_hash))));

    None
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
