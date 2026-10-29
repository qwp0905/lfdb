use std::{
  sync::{mpsc, Arc},
  thread,
  time::Duration,
};

use super::*;

const TABLE: TableId = 1;
const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq)]
enum Outcome {
  Hit(BlockId),
  Evicted { block_id: BlockId, evicted: bool },
}
impl Outcome {
  fn commit(acquired: Acquired<'_>) -> Self {
    match acquired {
      Acquired::Hit(block_id, _) => Self::Hit(block_id),
      Acquired::Evicted(guard) => {
        let outcome = Self::Evicted {
          block_id: guard.get_block_id(),
          evicted: guard.is_evicted(),
        };
        drop(guard.commit());
        outcome
      }
    }
  }
}

/**
 * A broken rollback leaves the acquire spinning, and a spinning thread cannot be
 * stopped, so the wait is bounded to report a failure instead of hanging.
 */
fn acquire_within(table: &Arc<MappingTable>, pointer: Pointer) -> Outcome {
  let (tx, rx) = mpsc::channel();
  let table = table.clone();
  thread::spawn(move || {
    let _ = tx.send(Outcome::commit(table.acquire(TABLE, pointer)));
  });
  rx.recv_timeout(TIMEOUT).expect("acquire did not finish")
}

fn reserve(table: &MappingTable, pointer: Pointer) -> EvictionGuard<'_> {
  let Acquired::Evicted(guard) = table.acquire(TABLE, pointer) else {
    panic!("pointer {pointer} is expected to miss");
  };
  guard
}

#[test]
fn test_mapping_table_commit_then_hit() {
  let table = MappingTable::new(1, 1);
  let Outcome::Evicted { block_id, evicted } = Outcome::commit(table.acquire(TABLE, 0))
  else {
    panic!("first acquire must miss");
  };
  assert!(!evicted);

  assert_eq!(
    Outcome::commit(table.acquire(TABLE, 0)),
    Outcome::Hit(block_id)
  );
}

#[test]
fn test_mapping_table_rollback_fresh_slot() {
  let table = MappingTable::new(1, 1);
  let guard = reserve(&table, 0);
  assert!(!guard.is_evicted());
  let block_id = guard.get_block_id();
  drop(guard);

  let guard = reserve(&table, 0);
  assert_eq!(guard.get_block_id(), block_id);
  assert!(!guard.is_evicted());
}

#[test]
fn test_mapping_table_rollback_evicted_slot() {
  let table = Arc::new(MappingTable::new(1, 1));
  let Outcome::Evicted { block_id, .. } = Outcome::commit(table.acquire(TABLE, 0)) else {
    panic!("first acquire must miss");
  };

  let guard = reserve(&table, 1);
  assert!(guard.is_evicted());
  drop(guard);

  assert_eq!(
    acquire_within(&table, 1),
    Outcome::Evicted {
      block_id,
      evicted: true
    }
  );
  assert_eq!(
    acquire_within(&table, 0),
    Outcome::Evicted {
      block_id,
      evicted: true
    }
  );
}

#[test]
fn test_mapping_table_rollback_keeps_evicted_key_reachable() {
  let table = Arc::new(MappingTable::new(1, 1));
  let Outcome::Evicted { block_id, .. } = Outcome::commit(table.acquire(TABLE, 0)) else {
    panic!("first acquire must miss");
  };
  drop(reserve(&table, 1));

  assert_eq!(acquire_within(&table, 0), Outcome::Hit(block_id));
  assert_eq!(
    acquire_within(&table, 1),
    Outcome::Evicted {
      block_id,
      evicted: true
    }
  );
}

#[test]
fn test_mapping_table_block_ids_skip_free_slots() {
  let table = MappingTable::new(1, 2);
  let Outcome::Evicted { block_id, .. } = Outcome::commit(table.acquire(TABLE, 0)) else {
    panic!("first acquire must miss");
  };
  drop(reserve(&table, 1));

  assert_eq!(table.get_block_ids().collect::<Vec<_>>(), vec![block_id]);
}

#[test]
fn test_mapping_table_no_evictable_slot() {
  let table = MappingTable::new(1, 1);
  let _guard = reserve(&table, 0);

  assert!(table.try_acquire(TABLE, 1).is_none());
}

#[test]
fn test_mapping_table_rollback_slot_pinned_by_flusher() {
  let table = Arc::new(MappingTable::new(1, 1));
  let Outcome::Evicted { block_id, .. } = Outcome::commit(table.acquire(TABLE, 0)) else {
    panic!("first acquire must miss");
  };
  drop(reserve(&table, 1));

  let pinned = table.try_pin(block_id).unwrap();
  let (tx, rx) = mpsc::channel();
  {
    let table = table.clone();
    thread::spawn(move || {
      let _ = tx.send(Outcome::commit(table.acquire(TABLE, 2)));
    });
  }
  assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
  drop(pinned);

  assert_eq!(
    rx.recv_timeout(TIMEOUT).expect("acquire did not finish"),
    Outcome::Evicted {
      block_id,
      evicted: true
    }
  );
}
