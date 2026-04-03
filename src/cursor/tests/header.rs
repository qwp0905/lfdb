use crate::{disk::Page, serialize::SerializeFrom};

use super::*;

#[test]
fn test_tree_header_roundtrip() {
  let mut page = Page::new();
  let height = 0u16;
  let root = 42;
  let mut header = TreeHeader::new(root);
  header.height = height;
  page.serialize_from(&header).expect("serialize error");

  let decoded: TreeHeader = page.deserialize().expect("deserialize error");
  assert_eq!(decoded.get_root(), root);
  assert_eq!(decoded.get_height(), height);
}

#[test]
fn test_tree_header_zero_root() {
  let mut page = Page::new();
  let height = 123u16;
  let root = 0;
  let mut header = TreeHeader::new(root);
  header.height = height;
  page.serialize_from(&header).expect("serialize error");

  let decoded: TreeHeader = page.deserialize().expect("deserialize error");
  assert_eq!(decoded.get_root(), root);
  assert_eq!(decoded.get_height(), height);
}

#[test]
fn test_tree_header_large_root() {
  let mut page = Page::new();
  let height = u16::MAX;
  let root = Pointer::MAX;
  let mut header = TreeHeader::new(root);
  header.height = height;
  page.serialize_from(&header).expect("serialize error");

  let decoded: TreeHeader = page.deserialize().expect("deserialize error");
  assert_eq!(decoded.get_root(), root);
  assert_eq!(decoded.get_height(), height);
}
