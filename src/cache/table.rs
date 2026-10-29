use std::{
  collections::BTreeMap,
  hash::{BuildHasher, RandomState},
  mem::ManuallyDrop,
  sync::{Mutex, MutexGuard},
};

use crossbeam::utils::Backoff;

use super::{CacheNode, Evicted, GetOrEvicted, ShrinkSet};
use crate::{
  background::ThreadParker,
  disk::Pointer,
  table::TableId,
  utils::{ChunkQueue, ExclusivePin, ExclusiveToken, SharedToken, ShortenedMutex},
};

type Key = (TableId, Pointer);

pub type BlockId = usize;

const U32_MASK: u64 = u32::MAX as u64;

struct Shard {
  node: CacheNode<Key, BlockId>,
  eviction: ShrinkSet<Key>, // evicting pointers
  allocated: BlockId,

  /**
   * Uncommitted fresh slots with no live key, reusable by a later miss.
   */
  free: ChunkQueue<BlockId>,
  /**
   * Slots whose eviction did not commit. They still hold the evicted key's latest
   * page, so the key is served from here instead of reading a stale disk block,
   * while a miss for another key must evict it again before reuse. Kept out of
   * the node so a failed eviction does not put the key back into the cache.
   */
  aborted: BTreeMap<Key, (BlockId, u64)>,
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
 * free slots, or to the aborted slots if an old key was evicted.
 */
pub struct EvictionGuard<'a> {
  evicted: Option<(Key, u64)>,
  block_id: BlockId,
  token: ManuallyDrop<ExclusiveToken<'a>>,
  selected: SelectedShard<'a>,
  new_key: Key,
  hasher: &'a RandomState,
}

impl<'a> EvictionGuard<'a> {
  const fn new(
    evicted: Option<(Key, u64)>,
    block_id: usize,
    token: ExclusiveToken<'a>,
    selected: SelectedShard<'a>,
    new_key: Key,
    hasher: &'a RandomState,
  ) -> Self {
    Self {
      evicted,
      block_id,
      token: ManuallyDrop::new(token),
      selected,
      new_key,
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
  pub fn commit(mut self) -> MappingToken<'a> {
    let token = unsafe { ManuallyDrop::take(&mut self.token) }.downgrade();
    {
      let mut guard = self.selected.shard.l();
      if let Some((k, h)) = self.evicted {
        guard.eviction.remove(h, &k, self.hasher);
      }
    };
    let parker = self.selected.parker;
    let _ = ManuallyDrop::new(self);
    MappingToken::new(token, parker)
  }
}
impl<'a> Drop for EvictionGuard<'a> {
  fn drop(&mut self) {
    let mut guard = self.selected.shard.l();
    if let Some((k, h)) = self.evicted {
      guard.eviction.remove(h, &k, self.hasher);
    }

    // rollback
    match self.evicted {
      Some((k, h)) => {
        guard.aborted.insert(k, (self.block_id, h));
      }
      None => guard.free.push(self.block_id),
    }
    guard
      .node
      .remove(&self.new_key, self.selected.hash, self.hasher)
      .unwrap_or_else(|| unreachable!());
    // No ownership claimed — block is immediately available for eviction.
    unsafe { ManuallyDrop::drop(&mut self.token) };
    self.selected.parker.wake_once();
  }
}

pub enum Acquired<'a> {
  Hit(BlockId, MappingToken<'a>),
  Evicted(EvictionGuard<'a>),
}

#[derive(Clone, Copy)]
struct SelectedShard<'a> {
  shard: &'a Mutex<Shard>,
  parker: &'a ThreadParker,
  offset: usize,
  hash: u64,
}

pub struct MappingToken<'a> {
  inner: ManuallyDrop<SharedToken<'a>>,
  parker: &'a ThreadParker,
}
impl<'a> MappingToken<'a> {
  const fn new(token: SharedToken<'a>, parker: &'a ThreadParker) -> Self {
    Self {
      inner: ManuallyDrop::new(token),
      parker,
    }
  }
}
impl<'a> Drop for MappingToken<'a> {
  fn drop(&mut self) {
    let token = unsafe { ManuallyDrop::take(&mut self.inner) };
    if token.drop_in_place() {
      self.parker.wake_once();
    }
  }
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
  parkers: Box<[ThreadParker]>,
  pins: Box<[ExclusivePin]>,
  hasher: RandomState,
  cap_per_shard: usize,
}
impl MappingTable {
  pub fn new(shard_count: usize, capacity: usize) -> Self {
    let cap_per_shard = capacity / shard_count;
    let mut shards = Vec::with_capacity(shard_count);
    let mut offset = Vec::with_capacity(shard_count);
    let mut parkers = Vec::with_capacity(shard_count);
    for i in 0..shard_count {
      let shard = Shard {
        node: CacheNode::new(cap_per_shard),
        eviction: ShrinkSet::new(),
        allocated: 0,
        free: ChunkQueue::new(),
        aborted: BTreeMap::new(),
      };
      shards.push(Mutex::new(shard));
      offset.push(i * cap_per_shard);
      parkers.push(ThreadParker::new());
    }

    let mut pins = Vec::with_capacity(capacity);
    pins.resize_with(capacity, ExclusivePin::new);

    Self {
      shards: shards.into_boxed_slice(),
      offsets: offset.into_boxed_slice(),
      parkers: parkers.into_boxed_slice(),
      pins: pins.into_boxed_slice(),
      hasher: RandomState::new(),
      cap_per_shard,
    }
  }
  fn select_shard(&self, key: Key) -> SelectedShard<'_> {
    let hash = self.hasher.hash_one(key);
    let i = (((hash & U32_MASK) * self.shards.len() as u64) >> 32) as usize;
    SelectedShard {
      shard: &self.shards[i],
      offset: self.offsets[i],
      parker: &self.parkers[i],
      hash,
    }
  }

  fn try_acquire_once<'a>(
    &'a self,
    selected: SelectedShard<'a>,
    key: Key,
  ) -> Option<Acquired<'a>> {
    let hasher = &self.hasher;
    let backoff = Backoff::new();
    let try_evict = |bid: &BlockId| self.pins[*bid].try_exclusive();
    loop {
      let mut shard = selected.shard.l();
      if shard.eviction.contains(selected.hash, &key) {
        drop(shard);
        backoff.snooze();
        continue;
      }

      let Ok(result) = shard
        .node
        .get_or_evict(&key, selected.hash, hasher, try_evict)
      else {
        return None;
      };
      let evicted = match result {
        GetOrEvicted::Hit(&bid) => {
          if let Some(token) = self.pins[bid].try_shared() {
            return Some(Acquired::Hit(
              bid,
              MappingToken::new(token, selected.parker),
            ));
          }
          drop(shard);
          backoff.snooze();
          continue;
        }
        GetOrEvicted::Evicted(evicted) => evicted,
      };

      if let Some(acquired) = self.handle_reserved(evicted, key, shard, selected) {
        return Some(acquired);
      };
      backoff.snooze();
    }
  }

  pub fn try_pin(&self, block_id: BlockId) -> Option<MappingToken<'_>> {
    let token = self.pins[block_id].try_shared()?;
    let parker = &self.parkers[block_id / self.cap_per_shard];
    Some(MappingToken::new(token, parker))
  }

  pub fn try_acquire(&self, table_id: TableId, pointer: Pointer) -> Option<Acquired<'_>> {
    let key = (table_id, pointer);
    let selected = self.select_shard(key);
    let backoff = Backoff::new();
    while !backoff.is_completed() {
      if let Some(acquired) = self.try_acquire_once(selected, key) {
        return Some(acquired);
      };
      backoff.snooze();
    }
    None
  }

  /**
   * Acquire a cache slot for an existing logical block address.
   *
   * Hits return the mapped block id with a shared slot token. Misses reserve a
   * slot and return an eviction guard so the caller can populate the slot before
   * committing the mapping.
   */
  pub fn acquire(&self, table_id: TableId, pointer: Pointer) -> Acquired<'_> {
    let key = (table_id, pointer);
    let selected = self.select_shard(key);
    let backoff = Backoff::new();
    loop {
      while !backoff.is_completed() {
        if let Some(acquired) = self.try_acquire_once(selected, key) {
          return acquired;
        };
        backoff.snooze();
      }
      if let Some(acquired) = selected
        .parker
        .park_or_cancel(|| self.try_acquire_once(selected, key))
      {
        return acquired;
      }
      backoff.reset();
    }
  }

  fn handle_reserved<'a>(
    &'a self,
    mut evicted: Evicted<Key, BlockId, ExclusiveToken<'a>>,
    key: Key,
    mut shard: MutexGuard<'a, Shard>,
    selected: SelectedShard<'a>,
  ) -> Option<Acquired<'a>> {
    let toward = evicted.toward();
    if let Some((bid, _)) = shard.aborted.remove(&key) {
      // An aborted slot sits outside the node, so the node had room for this key
      // and must not have evicted anything to make it.
      debug_assert!(evicted.take_evicted().is_none());
      shard
        .node
        .insert_to(key, selected.hash, bid, &self.hasher, toward);
      let token = self.pins[bid].try_shared().unwrap();
      return Some(Acquired::Hit(
        bid,
        MappingToken::new(token, selected.parker),
      ));
    }

    if let Some((evicted, bid, token, evicted_hash)) = evicted.take_evicted() {
      // Reuse the evicted cache slot for the new key. The mapping is reserved now,
      // but the slot may still contain the old page until the caller finishes the
      // eviction/load work, so keep the old key blocked during the transition.
      shard
        .node
        .insert_to(key, selected.hash, bid, &self.hasher, toward);
      shard
        .eviction
        .insert_unchecked(evicted, evicted_hash, &self.hasher);
      return Some(Acquired::Evicted(EvictionGuard::new(
        Some((evicted, evicted_hash)),
        bid,
        token,
        selected,
        key,
        &self.hasher,
      )));
    }

    let (bid, evicted) = match shard.free.pop() {
      Some(bid) => (bid, None),
      None => match shard.aborted.pop_first() {
        Some((k, (bid, h))) => (bid, Some((k, h))),
        None => {
          let id = shard.allocated;
          shard.allocated += 1;
          debug_assert!(shard.allocated <= shard.node.capacity());
          (id + selected.offset, None)
        }
      },
    };
    shard
      .node
      .insert_to(key, selected.hash, bid, &self.hasher, toward);

    let Some((evicted, evicted_hash)) = evicted else {
      let token = self.pins[bid].try_exclusive().unwrap();
      return Some(Acquired::Evicted(EvictionGuard::new(
        None,
        bid,
        token,
        selected,
        key,
        &self.hasher,
      )));
    };

    if let Some(token) = self.pins[bid].try_exclusive() {
      shard
        .eviction
        .insert_unchecked(evicted, evicted_hash, &self.hasher);
      return Some(Acquired::Evicted(EvictionGuard::new(
        Some((evicted, evicted_hash)),
        bid,
        token,
        selected,
        key,
        &self.hasher,
      )));
    }

    // An aborted slot can still be pinned, for example by a checkpoint flush. Undo
    // the reservation and keep the slot aborted so the evicted page stays
    // reachable, and retry once the pin is released.
    shard.node.remove(&key, selected.hash, &self.hasher);
    shard.aborted.insert(evicted, (bid, evicted_hash));
    None
  }

  pub fn get_block_ids(&self) -> impl Iterator<Item = BlockId> + '_ {
    self
      .shards
      .iter()
      .enumerate()
      .map(|(i, shard)| (shard.l().allocated, self.offsets[i]))
      .flat_map(|(len, offset)| offset..offset + len)
  }
}

// Safe because all mutable access to MappingTable (which contains raw pointers)
// is guarded by a Mutex, and all public methods take &self.
unsafe impl Sync for MappingTable {}
unsafe impl Send for MappingTable {}

#[cfg(all(test, not(loom)))]
#[path = "tests/table.rs"]
mod tests;
