use std::hash::RandomState;

use super::*;

fn h(hasher: &RandomState, key: usize) -> u64 {
  hasher.hash_one(key)
}

#[test]
fn test_insert_and_get() {
  let mut shard = LRUShard::<usize, usize>::new(10);
  let hasher = RandomState::new();

  assert_eq!(shard.insert(1, 100, h(&hasher, 1), &hasher), None);
  assert_eq!(shard.insert(2, 200, h(&hasher, 2), &hasher), None);

  assert_eq!(shard.get(&1, h(&hasher, 1), &hasher), Some(&100));
  assert_eq!(shard.get(&2, h(&hasher, 2), &hasher), Some(&200));
  assert_eq!(shard.get(&3, h(&hasher, 3), &hasher), None);
  assert_eq!(shard.len(), 2);
}

#[test]
fn test_insert_duplicate_key() {
  let mut shard = LRUShard::<usize, usize>::new(10);
  let hasher = RandomState::new();

  assert_eq!(shard.insert(1, 100, h(&hasher, 1), &hasher), None);
  assert_eq!(shard.insert(1, 200, h(&hasher, 1), &hasher), Some(100));
  assert_eq!(shard.get(&1, h(&hasher, 1), &hasher), Some(&200));
  assert_eq!(shard.len(), 1);
}

#[test]
fn test_is_full() {
  let cap = 5;
  let mut shard = LRUShard::<usize, usize>::new(cap);
  let hasher = RandomState::new();

  for i in 0..cap {
    assert!(!shard.is_full());
    shard.insert(i, i * 10, h(&hasher, i), &hasher);
  }
  assert!(shard.is_full());
}

#[test]
fn test_evict_order() {
  let cap = 5;
  let mut shard = LRUShard::<usize, usize>::new(cap);
  let hasher = RandomState::new();

  // insert 0~4, all go to old
  for i in 0..cap {
    shard.insert(i, i * 10, h(&hasher, i), &hasher);
  }

  // evict all: oldest first (FIFO within old)
  let evicted: Vec<usize> = (0..cap)
    .map(|_| shard.evict(&hasher).unwrap().0 .0)
    .collect();
  assert_eq!(evicted, vec![0, 1, 2, 3, 4]);
  assert_eq!(shard.len(), 0);
}

#[test]
fn test_get_promotes_and_evict_order() {
  let cap = 5;
  let mut shard = LRUShard::<usize, usize>::new(cap);
  let hasher = RandomState::new();

  for i in 0..cap {
    shard.insert(i, i * 10, h(&hasher, i), &hasher);
  }

  // access 0 and 1 to promote them
  shard.get(&0, h(&hasher, 0), &hasher);
  shard.get(&1, h(&hasher, 1), &hasher);

  // evict all and collect keys
  let mut evicted = Vec::new();
  while let Some((k, _)) = shard.evict(&hasher) {
    evicted.push(k);
  }

  // non-accessed entries (2, 3, 4) should be evicted before accessed ones (0, 1)
  let pos = |k: usize| evicted.iter().position(|(e, _)| *e == k).unwrap();
  assert!(pos(2) < pos(0));
  assert!(pos(3) < pos(0));
  assert!(pos(4) < pos(0));
  assert!(pos(2) < pos(1));
  assert!(pos(3) < pos(1));
  assert!(pos(4) < pos(1));
}

#[test]
fn test_evict_empty() {
  let mut shard = LRUShard::<usize, usize>::new(10);
  let hasher = RandomState::new();

  assert!(shard.evict(&hasher).is_none());
}
