use super::*;

#[test]
fn test_borrow() {
  let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  let mut source = AlignedBuf::new(10);
  source.as_mut_slice().copy_from_slice(&data);
  let v = VecRef::copied(source);
  assert_eq!(borrow::Borrow::<[u8]>::borrow(&v), data.as_slice());
}

#[test]
fn test_hash() {
  let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  let mut source = AlignedBuf::new(10);
  source.as_mut_slice().copy_from_slice(&data);
  let v = VecRef::copied(source);

  let hasher = hash::RandomState::new();
  assert_eq!(
    hash::BuildHasher::hash_one(&hasher, &v),
    hash::BuildHasher::hash_one(&hasher, &data)
  );
}
