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
  let reserved = shard
    .get_or_reserve(&k, h(hasher, k), hasher, never_fail)
    .unwrap();
  match reserved {
    GetOrReserve::Hit(_) => panic!("must reserved"),
    GetOrReserve::Reserved(mut reserved) => {
      reserved.fulfill(v);
    }
  }
}
fn get<'a>(
  shard: &'a mut CacheShard<usize, usize>,
  hasher: &'a RandomState,
  k: usize,
) -> GetOrReserve<'a, usize, usize, ()> {
  shard
    .get_or_reserve(&k, h(hasher, k), hasher, never_fail)
    .unwrap()
}

#[test]
fn test_basic_reserve_and_get() {
  let mut shard = CacheShard::<usize, usize>::new(20);
  let hasher = RandomState::new();

  insert(&mut shard, &hasher, 1, 100);
  insert(&mut shard, &hasher, 2, 200);

  assert!(matches!(
    get(&mut shard, &hasher, 1),
    GetOrReserve::Hit(&100)
  ));
  assert!(matches!(
    get(&mut shard, &hasher, 2),
    GetOrReserve::Hit(&200)
  ));
  assert!(matches!(
    get(&mut shard, &hasher, 3),
    GetOrReserve::Reserved(_)
  ));
}

#[test]
fn test_remove() {
  let mut shard = CacheShard::<usize, usize>::new(20);
  let hasher = RandomState::new();

  insert(&mut shard, &hasher, 1, 100);
  assert_eq!(shard.remove(&1, h(&hasher, 1)), Some(100));
  assert_eq!(shard.remove(&1, h(&hasher, 1)), None);
  assert!(matches!(
    get(&mut shard, &hasher, 1),
    GetOrReserve::Reserved(_)
  ));
}
