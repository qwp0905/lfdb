use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use super::*;

fn make_table(shard_count: usize, capacity: usize) -> LRUTable {
  let page_pool = Arc::new(PagePool::new(100));
  LRUTable::new(page_pool, shard_count, capacity)
}

fn miss(table: &LRUTable, index: Pointer) -> EvictionGuard<'_> {
  match table.acquire(index) {
    Acquired::Evicted(guard) => guard,
    _ => panic!("expected miss for index {}", index),
  }
}

fn hit(table: &LRUTable, index: Pointer) -> Arc<FrameState> {
  match table.acquire(index) {
    Acquired::Hit(state) => state,
    _ => panic!("expected hit for index {}", index),
  }
}

#[test]
fn test_cache_miss_then_hit() {
  let table = make_table(1, 4);

  let mut guard = miss(&table, 42);
  let frame_id = guard.get_frame_id();
  assert!(guard.get_evicted_pointer().is_none());
  guard.commit();
  drop(guard);

  let state = hit(&table, 42);
  assert_eq!(state.get_frame_id(), frame_id);
  state.unpin();
}

#[test]
fn test_multiple_misses_no_eviction() {
  let cap = 4;
  let table = make_table(1, cap);

  let mut frame_ids = Vec::new();
  for i in 0..cap {
    let mut guard = miss(&table, i as Pointer);
    assert!(guard.get_evicted_pointer().is_none());
    frame_ids.push(guard.get_frame_id());
    guard.commit();
  }

  frame_ids.sort();
  frame_ids.dedup();
  assert_eq!(frame_ids.len(), cap);
}

#[test]
fn test_eviction_when_full() {
  let cap = 4;
  let table = make_table(1, cap);

  for i in 0..cap {
    let mut guard = miss(&table, i as Pointer);
    assert!(guard.get_evicted_pointer().is_none());
    guard.commit();
  }

  // unpin all so eviction can happen
  for i in 0..cap {
    let state = hit(&table, i as Pointer);
    state.unpin(); // pin=1 from commit
    state.unpin(); // pin=0
  }

  let mut guard = miss(&table, 100);
  let evicted = guard.get_evicted_pointer();
  assert!(evicted.is_some());
  assert!(evicted.unwrap() < cap as Pointer);
  guard.commit();
}

#[test]
fn test_sharded_cache_hit() {
  let table = make_table(4, 80);

  let mut entries = Vec::new();
  for i in 0..16 {
    let mut guard = miss(&table, i);
    entries.push((i, guard.get_frame_id()));
    guard.commit();
  }

  for (index, expected_frame_id) in &entries {
    let state = hit(&table, *index);
    assert_eq!(state.get_frame_id(), *expected_frame_id);
    state.unpin();
  }
}

#[test]
fn test_sharded_frame_id_ranges() {
  let table = make_table(4, 80);

  let mut frame_ids = Vec::new();
  for i in 0..16 {
    let mut guard = miss(&table, i);
    frame_ids.push(guard.get_frame_id());
    guard.commit();
  }

  let mut sorted = frame_ids.clone();
  sorted.sort();
  sorted.dedup();
  assert_eq!(sorted.len(), frame_ids.len());

  for id in &frame_ids {
    assert!(*id < 80, "frame_id {} out of range", id);
  }
}

#[test]
fn test_sharded_eviction() {
  let table = make_table(4, 8);

  let mut inserted = Vec::new();
  let mut eviction_happened = false;
  for i in 0..20 {
    match table.acquire(i) {
      Acquired::Hit(state) => {
        state.unpin();
        continue;
      }
      Acquired::Evicted(mut guard) => {
        if guard.get_evicted_pointer().is_some() {
          eviction_happened = true;
        }
        inserted.push((i, guard.get_frame_id()));
        guard.commit();
      }
      Acquired::Temp(_) => {
        panic!("unexpected temp for index {}", i);
      }
    }
    // unpin so future evictions can proceed
    let state = hit(&table, i);
    state.unpin(); // from commit
    state.unpin(); // from this acquire
  }

  assert!(eviction_happened, "expected at least one eviction");

  for (_, frame_id) in &inserted {
    assert!(*frame_id < 8, "frame_id {} out of range", frame_id);
  }
}

#[test]
fn test_eviction_reuses_frame_id() {
  let cap = 4;
  let table = make_table(1, cap);

  for i in 0..cap {
    miss(&table, i as Pointer).commit();
  }

  for i in 0..cap {
    let state = hit(&table, i as Pointer);
    state.unpin();
    state.unpin();
  }

  let mut guard = miss(&table, 100);
  let new_frame_id = guard.get_frame_id();
  guard.commit();

  assert!(new_frame_id < cap);
}

#[test]
fn test_eviction_guard_drop_rollback() {
  let cap = 2;
  let table = make_table(1, cap);

  for i in [10, 20] {
    miss(&table, i).commit();
  }

  for i in [10, 20] {
    let state = hit(&table, i);
    state.unpin();
    state.unpin();
  }

  // evict but don't commit — Drop should rollback
  let guard = miss(&table, 30);
  let evicted = guard.get_evicted_pointer().unwrap();
  drop(guard);

  // evicted index should still be accessible (restored by rollback)
  let state = hit(&table, evicted);
  state.unpin();
}

#[test]
fn test_eviction_guard_commit_persists() {
  let cap = 2;
  let table = make_table(1, cap);

  for i in [10, 20] {
    miss(&table, i).commit();
  }

  for i in [10, 20] {
    let state = hit(&table, i);
    state.unpin();
    state.unpin();
  }

  let mut guard = miss(&table, 30);
  assert!(guard.get_evicted_pointer().is_some());
  guard.commit();
  drop(guard);

  let state = hit(&table, 30);
  state.unpin();
}

#[test]
fn test_pinned_entry_not_evicted() {
  let cap = 2;
  let table = make_table(1, cap);

  for i in [10, 20] {
    miss(&table, i).commit();
  }

  // keep index 10 pinned (pin=2 from commit+acquire), unpin 20
  let _pinned = hit(&table, 10); // pin=2
  let state20 = hit(&table, 20);
  state20.unpin(); // pin=1
  state20.unpin(); // pin=0

  // eviction should evict 20 (pin=0), not 10 (pin=2)
  let guard = miss(&table, 30);
  assert_eq!(guard.get_evicted_pointer().unwrap(), 20);
}

#[test]
fn test_concurrent_acquire_waits_for_eviction() {
  let cap = 2;
  let table = make_table(1, cap);

  for i in [10, 20] {
    miss(&table, i).commit();
  }
  for i in [10, 20] {
    let state = hit(&table, i);
    state.unpin();
    state.unpin();
  }

  // evict without commit — keeps pin=Eviction
  let mut guard = miss(&table, 30);
  let frame_id = guard.get_frame_id();

  let done = AtomicBool::new(false);

  thread::scope(|s| {
    s.spawn(|| {
      // acquire same index 30 — found in LRU but pin=Eviction, must wait
      let state = hit(&table, 30);
      done.store(true, Ordering::Release);
      state.unpin();
    });

    // give the thread time to enter backoff loop
    thread::sleep(Duration::from_millis(50));
    assert!(!done.load(Ordering::Acquire), "thread should be waiting");

    // commit + drop -> pin=Fetched(1) -> thread's try_pin succeeds
    guard.commit();
    drop(guard);
  });

  assert!(done.load(Ordering::Acquire), "thread should have completed");
  assert_eq!(hit(&table, 30).get_frame_id(), frame_id);
}

#[test]
fn test_concurrent_acquire_waits_for_evicted_index() {
  let cap = 2;
  let table = make_table(1, cap);

  for i in [10, 20] {
    miss(&table, i).commit();
  }
  for i in [10, 20] {
    let state = hit(&table, i);
    state.unpin();
    state.unpin();
  }

  // evict without commit — index 10 is in eviction set
  let guard = miss(&table, 30);
  let evicted = guard.get_evicted_pointer().unwrap();

  let done = AtomicBool::new(false);

  thread::scope(|s| {
    s.spawn(|| {
      // acquire the evicted index — should block on eviction set check
      let state = hit(&table, evicted);
      done.store(true, Ordering::Release);
      state.unpin();
    });

    // give the thread time to enter backoff loop
    thread::sleep(Duration::from_millis(50));
    assert!(!done.load(Ordering::Acquire), "thread should be waiting");

    // drop without commit -> rollback restores evicted index
    drop(guard);
  });

  assert!(done.load(Ordering::Acquire), "thread should have completed");
  // evicted index should be accessible after rollback
  let state = hit(&table, evicted);
  state.unpin();
}

#[test]
fn test_peek_or_temp_creates_temp_when_not_in_lru() {
  let table = make_table(1, 4);

  match table.peek_or_temp(42) {
    Peeked::Hit(_) => panic!("should return TempGuard"),
    Peeked::Temp(_) => panic!("should return TempGuard"),
    Peeked::DiskRead(guard) => {
      let (state, shard) = guard.take();
      state.completion_evict(1);
      state.unpin();
      shard.l().remove_temp(42);
    }
  }
}

#[test]
fn test_peek_or_temp_returns_lru_entry_without_promotion() {
  let table = make_table(1, 4);

  miss(&table, 10).commit();
  let state = hit(&table, 10);
  state.unpin();
  state.unpin();

  match table.peek_or_temp(10) {
    Peeked::Hit(state) => state.unpin(),
    _ => panic!("should return Hit from LRU"),
  }
}

#[test]
fn test_acquire_hits_temp_page() {
  let table = make_table(1, 4);

  // create temp page via peek_or_temp
  let guard = match table.peek_or_temp(42) {
    Peeked::DiskRead(g) => g,
    _ => panic!("should create temp via DiskRead"),
  };
  let (state, _) = guard.take();
  state.completion_evict(1);

  // acquire same index — should hit temp
  match table.acquire(42) {
    Acquired::Temp(temp) => {
      let (state, shard) = temp.take();
      state.unpin();
      state.unpin();
      shard.l().remove_temp(42);
    }
    _ => panic!("expected Acquired::Temp for index 42"),
  };
}

#[test]
fn test_remove_temp_then_peek_creates_new_temp() {
  let table = make_table(1, 4);

  // create and remove temp
  let (state, shard) = match table.peek_or_temp(42) {
    Peeked::DiskRead(g) => g.take(),
    _ => panic!("should create temp via DiskRead"),
  };
  state.completion_evict(1);
  state.unpin();
  shard.l().remove_temp(42);

  // peek again — should create new temp (DiskRead)
  match table.peek_or_temp(42) {
    Peeked::DiskRead(guard) => {
      let (state, shard) = guard.take();
      state.completion_evict(1);
      state.unpin();
      shard.l().remove_temp(42);
    }
    _ => panic!("should create new DiskRead"),
  }
}

#[test]
fn test_peek_or_temp_returns_existing_temp() {
  let table = make_table(1, 4);

  // create temp page
  let guard = match table.peek_or_temp(42) {
    Peeked::DiskRead(g) => g,
    _ => panic!("should create temp via DiskRead"),
  };
  let (state, _) = guard.take();
  state.completion_evict(1);

  // peek same index — should return existing temp (not DiskRead)
  match table.peek_or_temp(42) {
    Peeked::Temp(guard) => {
      let (state, shard) = guard.take();
      state.unpin();
      state.unpin();
      shard.l().remove_temp(42);
    }
    Peeked::DiskRead(_) => panic!("should return existing Temp, not DiskRead"),
    Peeked::Hit(_) => panic!("should return existing Temp, not Hit"),
  }
}
