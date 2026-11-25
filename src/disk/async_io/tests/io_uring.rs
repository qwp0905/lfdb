use std::rc::Rc;

use super::*;

const CAPACITY: usize = 4;
const TOTAL: usize = CAPACITY * 4;

/**
 * Completions arrive in any order, so each policy is checked while taking the
 * oldest, the newest, and the middle of the live tasks.
 */
const ORDERS: [fn(usize) -> usize; 3] = [|_| 0, |len| len - 1, |len| len / 2];

#[test]
fn test_pending_tasks_return_value_of_each_key() {
  for pick in ORDERS {
    let mut pendings = PendingTasks::with_capacity(CAPACITY);
    let mut live: Vec<_> = (0..TOTAL).map(|v| (pendings.insert(v), v)).collect();

    let mut inserted = TOTAL;
    while !live.is_empty() {
      let (key, value) = live.remove(pick(live.len()));
      assert_eq!(pendings.remove(key), Some(value));

      if inserted < TOTAL * 2 {
        live.push((pendings.insert(inserted), inserted));
        inserted += 1;
      }
      for (key, value) in &live {
        assert_ne!(*key, POLL);
        assert_eq!(pendings.get(*key), Some(value));
      }
    }
  }
}

#[test]
fn test_pending_tasks_drop_remaining_values_once() {
  for pick in ORDERS {
    let value = Rc::new(());
    let mut pendings = PendingTasks::with_capacity(CAPACITY);
    let mut keys: Vec<_> = (0..TOTAL).map(|_| pendings.insert(value.clone())).collect();

    for _ in 0..TOTAL / 2 {
      let key = keys.remove(pick(keys.len()));
      assert!(pendings.remove(key).is_some());
    }
    assert_eq!(Rc::strong_count(&value), 1 + keys.len());

    drop(pendings);
    assert_eq!(Rc::strong_count(&value), 1);
  }
}
