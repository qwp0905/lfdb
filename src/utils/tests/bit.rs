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
  // assert_eq!(bits.len(), 5);
  assert!(bits.remove(0));
  // assert_eq!(bits.len(), 4);
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
  let mut bits = OffsetBitmap::new(100, 10);
  assert_eq!(bits.insert(0), false);
  assert_eq!(bits.insert(100), true);
  assert_eq!(bits.insert(100), false);
  assert_eq!(bits.contains(100), true);
  assert_eq!(bits.contains(101), false);
  assert_eq!(bits.insert(200), false);
}
