use super::*;

#[test]
fn test_new_empty() {
  let v: Vector<usize> = Vector::new();
  assert_eq!(v.len(), 0);
  assert_eq!(v.get(0), None);
}

#[test]
fn test_push_and_get() {
  let mut v = Vector::new();
  v.push(10);
  v.push(20);
  v.push(30);
  assert_eq!(v.len(), 3);
  assert_eq!(v.get(0), Some(&10));
  assert_eq!(v.get(1), Some(&20));
  assert_eq!(v.get(2), Some(&30));
  assert_eq!(v.get(3), None);
}

#[test]
fn test_index() {
  let mut v = Vector::new();
  v.push(42);
  v.push(99);
  assert_eq!(v[0], 42);
  assert_eq!(v[1], 99);
}

#[test]
#[should_panic(expected = "index out of bounds")]
fn test_index_out_of_bounds() {
  let v: Vector<usize> = Vector::new();
  let _ = v[0];
}

#[test]
fn test_with_capacity() {
  let mut v: Vector<usize> = Vector::with_capacity(8);
  assert_eq!(v.len(), 0);
  for i in 0..8 {
    v.push(i);
  }
  assert_eq!(v.len(), 8);
  for i in 0..8 {
    assert_eq!(v[i], i);
  }
}

#[test]
fn test_grow_through_all_stack_slots() {
  let mut v = Vector::new();
  for i in 0..256 {
    v.push(i);
  }
  assert_eq!(v.len(), 256);
  for i in 0..256 {
    assert_eq!(v[i], i);
  }
}

#[test]
fn test_grow_to_heap() {
  let mut v = Vector::new();
  for i in 0..300 {
    v.push(i);
  }
  assert_eq!(v.len(), 300);
  for i in 0..300 {
    assert_eq!(v[i], i);
  }
}

#[test]
fn test_heap_with_capacity() {
  let mut v: Vector<usize> = Vector::with_capacity(512);
  for i in 0..512 {
    v.push(i);
  }
  assert_eq!(v.len(), 512);
  assert_eq!(v[0], 0);
  assert_eq!(v[511], 511);
}

#[test]
fn test_drop_elements() {
  use std::sync::atomic::{AtomicUsize, Ordering};
  static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

  #[derive(Debug)]
  struct DropCounter;
  impl Drop for DropCounter {
    fn drop(&mut self) {
      DROP_COUNT.fetch_add(1, Ordering::SeqCst);
    }
  }

  DROP_COUNT.store(0, Ordering::SeqCst);
  {
    let mut v = Vector::new();
    v.push(DropCounter);
    v.push(DropCounter);
    v.push(DropCounter);
  }
  assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 3);
}

#[test]
fn test_drop_after_grow() {
  use std::sync::atomic::{AtomicUsize, Ordering};
  static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

  #[derive(Debug)]
  struct DropCounter;
  impl Drop for DropCounter {
    fn drop(&mut self) {
      DROP_COUNT.fetch_add(1, Ordering::SeqCst);
    }
  }

  DROP_COUNT.store(0, Ordering::SeqCst);
  {
    let mut v = Vector::new();
    // push 5 → grows through S1, S2, S4, S8
    for _ in 0..5 {
      v.push(DropCounter);
    }
  }
  assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);
}

#[test]
fn test_push_single() {
  let mut v = Vector::new();
  v.push(1);
  assert_eq!(v.len(), 1);
  assert_eq!(v[0], 1);
}

#[test]
fn test_boundary_sizes() {
  for size in [1, 2, 4, 8, 16, 32, 64, 128, 256] {
    let mut v = Vector::new();
    for i in 0..size {
      v.push(i);
    }
    assert_eq!(v.len(), size);
    for i in 0..size {
      assert_eq!(v[i], i);
    }
  }
}
