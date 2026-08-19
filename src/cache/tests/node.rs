use std::hash::RandomState;

use super::*;

fn h(hasher: &RandomState, key: usize) -> u64 {
  hasher.hash_one(key)
}

fn never_fail<V>(_: &V) -> Option<()> {
  Some(())
}

fn insert(node: &mut CacheNode<usize, usize>, hasher: &RandomState, k: usize, v: usize) {
  let h = h(hasher, k);
  let reserved = node.get_or_reserve(&k, h, hasher, never_fail).unwrap();
  match reserved {
    GetOrEvicted::Hit(_) => panic!("must reserved"),
    GetOrEvicted::Evicted(evicted) => {
      node.insert_to(&k, h, v, hasher, evicted.toward);
    }
  }
}
fn get<'a>(
  node: &'a mut CacheNode<usize, usize>,
  hasher: &'a RandomState,
  k: usize,
) -> GetOrEvicted<'a, usize, usize, ()> {
  node
    .get_or_reserve(&k, h(hasher, k), hasher, never_fail)
    .unwrap()
}

#[test]
fn test_basic_reserve_and_get() {
  let mut node = CacheNode::<usize, usize>::new(20);
  let hasher = RandomState::new();

  insert(&mut node, &hasher, 1, 100);
  insert(&mut node, &hasher, 2, 200);

  assert!(matches!(
    get(&mut node, &hasher, 1),
    GetOrEvicted::Hit(&100)
  ));
  assert!(matches!(
    get(&mut node, &hasher, 2),
    GetOrEvicted::Hit(&200)
  ));
  assert!(matches!(
    get(&mut node, &hasher, 3),
    GetOrEvicted::Evicted(_)
  ));
}

#[test]
fn test_remove() {
  let mut node = CacheNode::<usize, usize>::new(20);
  let hasher = RandomState::new();

  insert(&mut node, &hasher, 1, 100);
  assert_eq!(node.remove(&1, h(&hasher, 1), &hasher), Some(100));
  assert_eq!(node.remove(&1, h(&hasher, 1), &hasher), None);
  assert!(matches!(
    get(&mut node, &hasher, 1),
    GetOrEvicted::Evicted(_)
  ));
}
