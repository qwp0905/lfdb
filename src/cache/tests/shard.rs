use std::hash::RandomState;

use super::*;

fn h(hasher: &RandomState, key: usize) -> u64 {
  hasher.hash_one(key)
}

fn never_fail<V>(_: &V) -> Option<()> {
  Some(())
}

fn insert(
  shard: &mut CacheShard<usize, usize>,
  hasher: &RandomState,
  k: usize,
  v: usize,
) {
  let mut reserved = shard
    .reserve_key(k, h(hasher, k), hasher, never_fail)
    .unwrap();
  let _ = reserved.take_evicted();
  reserved.fulfill(v);
}

#[test]
fn test_basic_reserve_and_get() {
  let mut shard = CacheShard::<usize, usize>::new(20);
  let hasher = RandomState::new();

  insert(&mut shard, &hasher, 1, 100);
  insert(&mut shard, &hasher, 2, 200);

  assert_eq!(shard.get(&1, h(&hasher, 1)), Some(&100));
  assert_eq!(shard.get(&2, h(&hasher, 2)), Some(&200));
  assert_eq!(shard.get(&3, h(&hasher, 3)), None);
}

#[test]
fn test_remove() {
  let mut shard = CacheShard::<usize, usize>::new(20);
  let hasher = RandomState::new();

  insert(&mut shard, &hasher, 1, 100);
  assert_eq!(shard.remove(&1, h(&hasher, 1)), Some(100));
  assert_eq!(shard.get(&1, h(&hasher, 1)), None);
  assert_eq!(shard.remove(&1, h(&hasher, 1)), None);
}

#[test]
fn test_len_tracks_active_only() {
  let mut shard = CacheShard::<usize, usize>::new(20);
  let hasher = RandomState::new();

  assert_eq!(shard.len(), 0);
  insert(&mut shard, &hasher, 1, 100);
  assert_eq!(shard.len(), 1);
  insert(&mut shard, &hasher, 2, 200);
  assert_eq!(shard.len(), 2);

  // remove drops active count (Tombstone is not active)
  shard.remove(&1, h(&hasher, 1));
  assert_eq!(shard.len(), 1);
}

#[test]
fn test_peek_does_not_promote() {
  // peek does not bump freq, so the entry stays unpromoted and gets demoted on small evict.
  // capacity=20 → small_cap=2
  let mut shard = CacheShard::<usize, usize>::new(20);
  let hasher = RandomState::new();

  insert(&mut shard, &hasher, 1, 100);

  // multiple peeks keep freq at 0
  for _ in 0..5 {
    assert_eq!(shard.peek(&1, h(&hasher, 1)), Some(&100));
  }

  // fill small to push key=1 out
  for i in 100..110 {
    insert(&mut shard, &hasher, i, i);
  }

  // demoted to ghost → get returns None
  assert_eq!(shard.get(&1, h(&hasher, 1)), None);
}

#[test]
fn test_get_promotes_to_main() {
  // get raises freq enough to trigger promotion to main on small evict
  let mut shard = CacheShard::<usize, usize>::new(20);
  let hasher = RandomState::new();

  insert(&mut shard, &hasher, 1, 100);

  // get twice → freq > 1
  shard.get(&1, h(&hasher, 1));
  shard.get(&1, h(&hasher, 1));

  // fill small to push key=1 out
  for i in 100..110 {
    insert(&mut shard, &hasher, i, i);
  }

  // promoted to main → still accessible
  assert_eq!(shard.get(&1, h(&hasher, 1)), Some(&100));
}

#[test]
fn test_demote_returns_evicted() {
  // when key=1 is demoted from small to ghost, the next reserve_key should
  // surface key=1 as the evicted entry
  let mut shard = CacheShard::<usize, usize>::new(20);
  let hasher = RandomState::new();

  insert(&mut shard, &hasher, 1, 100);
  insert(&mut shard, &hasher, 2, 200);

  // small is now at capacity (small_cap=2). Next insert triggers evict.
  let mut reserved = shard
    .reserve_key(3, h(&hasher, 3), &hasher, never_fail)
    .unwrap();
  let evicted = reserved.take_evicted();
  reserved.fulfill(300);

  // FIFO: key=1 is evicted first (freq=0 demote)
  let (k, v, _) = evicted.expect("eviction expected");
  assert_eq!(k, 1);
  assert_eq!(v, 100);
}

#[test]
fn test_ghost_hit_promotes_to_main() {
  // re-insert of a key demoted from S goes directly to M (ghost hit path)
  let mut shard = CacheShard::<usize, usize>::new(20);
  let hasher = RandomState::new();

  insert(&mut shard, &hasher, 1, 100);

  // fill small enough to demote key=1 to ghost
  for i in 100..110 {
    insert(&mut shard, &hasher, i, i);
  }
  assert_eq!(shard.get(&1, h(&hasher, 1)), None);

  // re-insert key=1 → ghost hit → goes to main directly
  insert(&mut shard, &hasher, 1, 999);

  // pushing more entries to small does not evict key=1 (it lives in main)
  for i in 200..210 {
    insert(&mut shard, &hasher, i, i);
  }
  assert_eq!(shard.get(&1, h(&hasher, 1)), Some(&999));
}

#[test]
fn test_main_holds_hot_keys() {
  // keys promoted to main survive a flood of new entries
  let mut shard = CacheShard::<usize, usize>::new(20);
  let hasher = RandomState::new();

  // insert 1, 2 and access them enough to be promoted to main
  for k in [1, 2] {
    insert(&mut shard, &hasher, k, k * 100);
    shard.get(&k, h(&hasher, k));
    shard.get(&k, h(&hasher, k));
  }

  // fill small so 1 and 2 are promoted
  for i in 100..120 {
    insert(&mut shard, &hasher, i, i);
  }

  // hot keys preserved in main
  assert_eq!(shard.get(&1, h(&hasher, 1)), Some(&100));
  assert_eq!(shard.get(&2, h(&hasher, 2)), Some(&200));
}

#[test]
fn test_remove_evicted_returns_none() {
  // a key already demoted to ghost has no value to return on remove
  let mut shard = CacheShard::<usize, usize>::new(20);
  let hasher = RandomState::new();

  insert(&mut shard, &hasher, 1, 100);
  for i in 100..110 {
    insert(&mut shard, &hasher, i, i);
  }

  // ghost-state key remove → None
  assert_eq!(shard.remove(&1, h(&hasher, 1)), None);
}

#[test]
fn test_freq_saturates() {
  // saturating freq means many get calls do not overflow or panic
  let mut shard = CacheShard::<usize, usize>::new(20);
  let hasher = RandomState::new();

  insert(&mut shard, &hasher, 1, 100);

  for _ in 0..100 {
    assert_eq!(shard.get(&1, h(&hasher, 1)), Some(&100));
  }
}

#[test]
fn test_evict_can_reject() {
  // when the evict closure returns None, reserve_key fails (no eviction performed)
  let mut shard = CacheShard::<usize, usize>::new(20);
  let hasher = RandomState::new();

  // fill small to capacity (small_cap = 2)
  insert(&mut shard, &hasher, 1, 100);
  insert(&mut shard, &hasher, 2, 200);

  // attempt insert with a rejecting evict closure → Err
  let result = shard.reserve_key(3, h(&hasher, 3), &hasher, |_: &usize| -> Option<()> {
    None
  });
  assert!(result.is_err());

  // existing entries remain
  assert_eq!(shard.get(&1, h(&hasher, 1)), Some(&100));
  assert_eq!(shard.get(&2, h(&hasher, 2)), Some(&200));
}

#[test]
fn test_drop_no_leak() {
  // verify drop runs cleanly across all four states (Small / Main / Ghost / Tombstone).
  // best validated with miri; this test just exercises the drop path.
  let mut shard = CacheShard::<usize, Box<usize>>::new(20);
  let hasher = RandomState::new();

  for i in 0..15 {
    let mut r = shard
      .reserve_key(i, h(&hasher, i), &hasher, never_fail)
      .unwrap();
    let _ = r.take_evicted();
    r.fulfill(Box::new(i * 10));
  }

  // promote some
  shard.get(&0, h(&hasher, 0));
  shard.get(&0, h(&hasher, 0));
  shard.get(&1, h(&hasher, 1));
  shard.get(&1, h(&hasher, 1));

  // additional inserts demote some to ghost
  for i in 100..110 {
    let mut r = shard
      .reserve_key(i, h(&hasher, i), &hasher, never_fail)
      .unwrap();
    let _ = r.take_evicted();
    r.fulfill(Box::new(i));
  }

  // create tombstones
  shard.remove(&0, h(&hasher, 0));
  shard.remove(&100, h(&hasher, 100));

  drop(shard);
}
