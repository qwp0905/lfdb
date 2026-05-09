use super::*;
use std::cell::Cell;
use std::rc::Rc;
struct Tracker {
  counter: Rc<Cell<usize>>,
}
impl Tracker {
  fn new(counter: Rc<Cell<usize>>) -> Self {
    Self { counter }
  }
}
impl Drop for Tracker {
  fn drop(&mut self) {
    self.counter.set(self.counter.get() + 1);
  }
}
impl Clone for Tracker {
  fn clone(&self) -> Self {
    Self::new(self.counter.clone())
  }
}
#[test]
fn test_new_is_empty() {
  let v: InlineVec<i32, 4> = InlineVec::new();
  assert_eq!(v.len(), 0);
}
#[test]
fn test_push_pop_inline() {
  let mut v: InlineVec<i32, 4> = InlineVec::new();
  v.push(1);
  v.push(2);
  v.push(3);
  assert_eq!(v.len(), 3);
  assert_eq!(v.pop(), Some(3));
  assert_eq!(v.pop(), Some(2));
  assert_eq!(v.pop(), Some(1));
  assert_eq!(v.pop(), None);
}
#[test]
fn test_push_until_full_inline() {
  let mut v: InlineVec<i32, 4> = InlineVec::new();
  for i in 0..4 {
    v.push(i);
  }
  assert_eq!(v.len(), 4);
  assert_eq!(&*v, &[0, 1, 2, 3]);
}
#[test]
fn test_push_beyond_n() {
  let mut v: InlineVec<i32, 4> = InlineVec::new();
  for i in 0..6 {
    v.push(i);
  }
  assert_eq!(v.len(), 6);
  assert_eq!(&*v, &[0, 1, 2, 3, 4, 5]);
}
#[test]
fn test_pop_after_promotion() {
  let mut v: InlineVec<i32, 2> = InlineVec::new();
  for i in 0..5 {
    v.push(i);
  }
  assert_eq!(v.pop(), Some(4));
  assert_eq!(v.pop(), Some(3));
  assert_eq!(v.pop(), Some(2));
  assert_eq!(v.pop(), Some(1));
  assert_eq!(v.pop(), Some(0));
  assert_eq!(v.pop(), None);
}
#[test]
fn test_index_usize() {
  let mut v: InlineVec<i32, 4> = InlineVec::new();
  for i in 0..3 {
    v.push(i * 10);
  }
  assert_eq!(v[0], 0);
  assert_eq!(v[1], 10);
  assert_eq!(v[2], 20);
}
#[test]
#[should_panic]
fn test_index_oob() {
  let v: InlineVec<i32, 4> = InlineVec::new();
  let _ = v[0];
}
#[test]
fn test_index_mut() {
  let mut v: InlineVec<i32, 4> = InlineVec::new();
  v.push(1);
  v.push(2);
  v[0] = 100;
  assert_eq!(v[0], 100);
  assert_eq!(v[1], 2);
}
#[test]
fn test_index_range() {
  let mut v: InlineVec<i32, 4> = InlineVec::new();
  for i in 0..4 {
    v.push(i);
  }
  assert_eq!(&v[0..4], &[0, 1, 2, 3]);
  assert_eq!(&v[1..3], &[1, 2]);
}
#[test]
fn test_index_mut_range() {
  let mut v: InlineVec<u8, 5> = inline_vec!(1, 2, 3, 4, 5);
  v[2..4].copy_from_slice(&[6, 7]);
  assert_eq!(&*v, &[1, 2, 6, 7, 5])
}
#[test]
#[should_panic]
fn test_index_range_oob() {
  let mut v: InlineVec<i32, 4> = InlineVec::new();
  v.push(0);
  let _ = &v[0..2];
}
#[test]
fn test_from_slice_inline() {
  let v: InlineVec<i32, 8> = InlineVec::from(&[1, 2, 3][..]);
  assert_eq!(v.len(), 3);
  assert_eq!(&*v, &[1, 2, 3]);
}
#[test]
fn test_from_slice_overflow_to_heap() {
  let v: InlineVec<i32, 2> = InlineVec::from(&[1, 2, 3, 4, 5][..]);
  assert_eq!(v.len(), 5);
  assert_eq!(&*v, &[1, 2, 3, 4, 5]);
}
#[test]
fn test_from_vec() {
  let v: InlineVec<i32, 4> = InlineVec::from(vec![1, 2, 3]);
  assert_eq!(&*v, &[1, 2, 3]);
}
#[test]
fn test_macro_empty() {
  let v: InlineVec<i32, 4> = inline_vec![];
  assert_eq!(v.len(), 0);
}
#[test]
fn test_macro_elements() {
  let v: InlineVec<i32, 4> = inline_vec![1, 2, 3];
  assert_eq!(&*v, &[1, 2, 3]);
}
#[test]
fn test_macro_repeated() {
  let v: InlineVec<i32, 4> = inline_vec![7; 3];
  assert_eq!(v.len(), 3);
  assert_eq!(&*v, &[7, 7, 7]);
}
#[test]
fn test_into_iter_inline() {
  let mut v: InlineVec<i32, 4> = InlineVec::new();
  for i in 1..=3 {
    v.push(i);
  }
  let collected: Vec<_> = v.into_iter().collect();
  assert_eq!(collected, vec![1, 2, 3]);
}
#[test]
fn test_into_iter_heap() {
  let v: InlineVec<i32, 2> = InlineVec::from(vec![1, 2, 3, 4]);
  let collected: Vec<_> = v.into_iter().collect();
  assert_eq!(collected, vec![1, 2, 3, 4]);
}
#[test]
fn test_into_iter_empty() {
  let v: InlineVec<i32, 4> = InlineVec::new();
  let collected: Vec<_> = v.into_iter().collect();
  assert!(collected.is_empty());
}
#[test]
fn test_drop_inline_all_elements() {
  let counter = Rc::new(Cell::new(0));
  {
    let mut v: InlineVec<Tracker, 4> = InlineVec::new();
    v.push(Tracker::new(counter.clone()));
    v.push(Tracker::new(counter.clone()));
    v.push(Tracker::new(counter.clone()));
    assert_eq!(counter.get(), 0);
  }
  assert_eq!(counter.get(), 3);
}
#[test]
fn test_drop_heap_all_elements() {
  let counter = Rc::new(Cell::new(0));
  {
    let mut v: InlineVec<Tracker, 2> = InlineVec::new();
    for _ in 0..5 {
      v.push(Tracker::new(counter.clone()));
    }
    assert_eq!(counter.get(), 0);
  }
  assert_eq!(counter.get(), 5);
}
#[test]
fn test_drop_pop_inline() {
  let counter = Rc::new(Cell::new(0));
  let mut v: InlineVec<Tracker, 4> = InlineVec::new();
  v.push(Tracker::new(counter.clone()));
  v.push(Tracker::new(counter.clone()));
  let popped = v.pop().unwrap();
  assert_eq!(counter.get(), 0);
  drop(popped);
  assert_eq!(counter.get(), 1);
  drop(v);
  assert_eq!(counter.get(), 2);
}
#[test]
fn test_drop_pop_heap() {
  let counter = Rc::new(Cell::new(0));
  let mut v: InlineVec<Tracker, 2> = InlineVec::new();
  for _ in 0..5 {
    v.push(Tracker::new(counter.clone()));
  }
  let popped = v.pop().unwrap();
  assert_eq!(counter.get(), 0);
  drop(popped);
  assert_eq!(counter.get(), 1);
  drop(v);
  assert_eq!(counter.get(), 5);
}
#[test]
fn test_drop_no_double_drop_on_promotion() {
  let counter = Rc::new(Cell::new(0));
  {
    let mut v: InlineVec<Tracker, 2> = InlineVec::new();
    v.push(Tracker::new(counter.clone()));
    v.push(Tracker::new(counter.clone()));
    v.push(Tracker::new(counter.clone()));
    assert_eq!(v.len(), 3);
    assert_eq!(counter.get(), 0);
  }
  assert_eq!(counter.get(), 3);
}
#[test]
fn test_drop_no_double_drop() {
  let counter = Rc::new(Cell::new(0));
  {
    let mut v: InlineVec<Tracker, 4> = InlineVec::new();
    v.push(Tracker::new(counter.clone()));
    v.push(Tracker::new(counter.clone()));
    v.push(Tracker::new(counter.clone()));
    assert_eq!(v.len(), 3);
    assert_eq!(counter.get(), 0);
  }
  assert_eq!(counter.get(), 3);
}
#[test]
fn test_drop_no_leak_on_multiple_grows() {
  let counter = Rc::new(Cell::new(0));
  {
    let mut v: InlineVec<Tracker, 2> = InlineVec::new();
    for _ in 0..50 {
      v.push(Tracker::new(counter.clone()));
    }
    assert_eq!(counter.get(), 0);
  }
  assert_eq!(counter.get(), 50);
}
#[test]
fn test_drop_promotion_at_n_eq_1() {
  let counter = Rc::new(Cell::new(0));
  {
    let mut v: InlineVec<Tracker, 1> = InlineVec::new();
    v.push(Tracker::new(counter.clone()));
    v.push(Tracker::new(counter.clone()));
    assert_eq!(counter.get(), 0);
  }
  assert_eq!(counter.get(), 2);
}
#[test]
fn test_drop_into_iter_full_consume() {
  let counter = Rc::new(Cell::new(0));
  let mut v: InlineVec<Tracker, 4> = InlineVec::new();
  for _ in 0..3 {
    v.push(Tracker::new(counter.clone()));
  }
  let collected: Vec<_> = v.into_iter().collect();
  assert_eq!(counter.get(), 0);
  drop(collected);
  assert_eq!(counter.get(), 3);
}
#[test]
fn test_drop_into_iter_partial_inline() {
  let counter = Rc::new(Cell::new(0));
  let mut v: InlineVec<Tracker, 4> = InlineVec::new();
  for _ in 0..4 {
    v.push(Tracker::new(counter.clone()));
  }
  let mut iter = v.into_iter();
  let first = iter.next().unwrap();
  let second = iter.next().unwrap();
  assert_eq!(counter.get(), 0);
  drop(iter);
  assert_eq!(counter.get(), 2);
  drop(first);
  drop(second);
  assert_eq!(counter.get(), 4);
}
#[test]
fn test_drop_into_iter_partial_heap() {
  let counter = Rc::new(Cell::new(0));
  let mut v: InlineVec<Tracker, 2> = InlineVec::new();
  for _ in 0..5 {
    v.push(Tracker::new(counter.clone()));
  }
  let mut iter = v.into_iter();
  let first = iter.next().unwrap();
  drop(iter);
  assert_eq!(counter.get(), 4);
  drop(first);
  assert_eq!(counter.get(), 5);
}
#[test]
fn test_drop_from_vec() {
  let counter = Rc::new(Cell::new(0));
  {
    let trackers = (0..3)
      .map(|_| Tracker::new(counter.clone()))
      .collect::<Vec<_>>();
    let _v: InlineVec<Tracker, 4> = InlineVec::from(trackers);
    assert_eq!(counter.get(), 0);
  }
  assert_eq!(counter.get(), 3);
}
#[test]
fn test_drop_empty_vec() {
  let v: InlineVec<Tracker, 4> = InlineVec::new();
  drop(v);
}
#[test]
fn test_clone_without_promotion() {
  let counter = Rc::new(Cell::new(0));
  {
    let mut v: InlineVec<Tracker, 10> = inline_vec!();
    for _ in 0..5 {
      v.push(Tracker::new(counter.clone()))
    }
    let _vv = v.clone();
  }
  assert_eq!(counter.get(), 10);
}
#[test]
fn test_clone_on_promotion() {
  let counter = Rc::new(Cell::new(0));
  {
    let mut v: InlineVec<Tracker, 3> = inline_vec!();
    for _ in 0..5 {
      v.push(Tracker::new(counter.clone()))
    }
    let _vv = v.clone();
  }
  assert_eq!(counter.get(), 10);
}
