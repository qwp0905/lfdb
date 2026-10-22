use super::*;

#[test]
fn test_insert() {
  let bits = AtomicBitmap::new(100);
  assert!(bits.insert(0));
  assert!(bits.insert(1));
  assert!(bits.insert(63));
  assert!(bits.insert(64));
  assert!(bits.insert(65));
}

#[test]
fn test_contains() {
  let bits = AtomicBitmap::new(100);
  bits.insert(0);
  bits.insert(1);
  bits.insert(63);
  bits.insert(64);
  bits.insert(65);
  assert!(bits.contains(0));
  assert!(bits.contains(1));
  assert!(bits.contains(63));
  assert!(bits.contains(64));
  assert!(bits.contains(65));
  assert!(!bits.contains(2));
  assert!(!bits.contains(62));
  assert!(!bits.contains(66));
}

#[test]
fn test_remove() {
  let bits = AtomicBitmap::new(100);
  assert!(bits.insert(0));
  assert!(bits.insert(1));
  assert!(bits.insert(63));
  assert!(bits.insert(64));
  assert!(bits.insert(65));
  assert!(bits.remove(0));
  assert!(!bits.contains(0));
}

#[test]
fn test_iter() {
  let bits = AtomicBitmap::new(100);
  bits.insert(0);
  bits.insert(1);
  bits.insert(63);
  bits.insert(64);
  bits.insert(65);
  let mut iter = bits.iter();
  assert_eq!(iter.next(), Some(0));
  assert_eq!(iter.next(), Some(1));
  assert_eq!(iter.next(), Some(63));
  assert_eq!(iter.next(), Some(64));
  assert_eq!(iter.next(), Some(65));
  assert_eq!(iter.next(), None);
}

#[test]
fn test_offset_bit() {
  let mut bits = OffsetBitmap::new(100);
  assert!(bits.insert(100));
  assert!(!bits.insert(100));
  assert!(bits.contains(100),);
  assert!(!bits.contains(101),);
  assert!(bits.insert(200));
  assert!(bits.contains(200));
  assert!(!bits.contains(0));
  assert!(bits.insert(0));
  assert!(bits.contains(0));
  assert!(bits.contains(100));
  assert!(bits.contains(200));
}

#[test]
fn test_offset_bitmap_extend_left() {
  let mut bits = OffsetBitmap::new(0);
  assert!(bits.insert(70));
  assert!(bits.insert(60));
  assert!(bits.insert(1));
  assert!(!bits.insert(60));
  assert_eq!(bits.iter().collect::<Vec<_>>(), vec![1, 60, 70]);
  assert_eq!(bits.front(), Some(1));
}

#[test]
fn test_offset_bitmap_reanchor_after_empty() {
  let mut bits = OffsetBitmap::new(1);
  for i in 1..100_000 {
    assert!(bits.insert(i));
    assert!(bits.remove_and_shrink(i));
  }
  assert!(bits.insert(100_000));
  assert_eq!(bits.bits.len(), 1);
  assert_eq!(bits.front(), Some(100_000));
}

#[test]
fn test_offset_bitmap_remove_until_beyond_window() {
  let mut bits = OffsetBitmap::new(0);
  bits.insert(10);
  bits.remove_until(50);
  bits.remove_until(200);
  bits.remove_until(1000);
  assert!(!bits.contains(10));
  assert!(bits.insert(5));
  assert!(bits.insert(1500));
  assert_eq!(bits.iter().collect::<Vec<_>>(), vec![5, 1500]);
}

#[test]
fn test_offset_bitmap_front_after_remove_until() {
  let mut bits = OffsetBitmap::new(0);
  bits.insert(5);
  bits.insert(70);
  bits.remove_until(10);
  assert!(!bits.contains(5));
  assert!(bits.contains(70));
  assert_eq!(bits.front(), Some(70));
}

#[test]
fn test_offset_iter() {
  let mut bits = OffsetBitmap::new(123);
  bits.insert(123);
  bits.insert(124);
  bits.insert(134);
  bits.insert(231);
  bits.insert(190);
  let mut iter = bits.iter();
  assert_eq!(iter.next(), Some(123));
  assert_eq!(iter.next(), Some(124));
  assert_eq!(iter.next(), Some(134));
  assert_eq!(iter.next(), Some(190));
  assert_eq!(iter.next(), Some(231));
  assert_eq!(iter.next(), None);
}

#[test]
fn test_offset_bitmap_dynamic() {
  let mut bits = OffsetBitmap::new(0);
  assert!(bits.insert(100));

  for i in 101..983 {
    assert!(bits.insert(i));
  }

  bits.remove_until(333);
  for i in 100..333 {
    assert!(!bits.contains(i))
  }
}
