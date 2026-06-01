use crate::{disk::Page, objects::TypedObject};

use super::*;

#[test]
fn test_tree_header_roundtrip() {
  let mut page = Page::new();
  let height = 0u16;
  let root = 42;
  let mut obj: TypedObject = TreeHeader::new(root).into();
  let header = obj.as_tree_header_mut().unwrap();
  header.height = height;
  page.serialize_from(&obj).expect("serialize error");

  let obj = page.view().expect("deserialize error");
  let decoded = obj.as_tree_header().unwrap();
  assert_eq!(decoded.get_root(), root);
  assert_eq!(decoded.get_height(), height);
}

#[test]
fn test_tree_header_zero_root() {
  let mut page = Page::new();
  let height = 123u16;
  let root = 0;
  let mut obj: TypedObject = TreeHeader::new(root).into();
  let header = obj.as_tree_header_mut().unwrap();
  header.height = height;
  page.serialize_from(&obj).expect("serialize error");

  let obj = page.deserialize().expect("deserialize error");
  let decoded = obj.as_tree_header().unwrap();
  assert_eq!(decoded.get_root(), root);
  assert_eq!(decoded.get_height(), height);
}

#[test]
fn test_tree_header_large_root() {
  let mut page = Page::new();
  let height = u16::MAX;
  let root = Pointer::MAX;
  let mut obj = TypedObject::from(TreeHeader::new(root));
  let header = obj.as_tree_header_mut().unwrap();
  header.height = height;
  page.serialize_from(&obj).expect("serialize error");

  let obj = page.deserialize().expect("deserialize error");
  let decoded = obj.as_tree_header().unwrap();
  assert_eq!(decoded.get_root(), root);
  assert_eq!(decoded.get_height(), height);
}
