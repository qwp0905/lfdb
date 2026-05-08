use crate::{disk::Page, serialize::SerializeFrom};

use super::*;

#[test]
fn test_serialize_internal() {
  let keys = vec![];
  let children = vec![10];
  let next = None;
  let mut page = Page::new();
  let node = BTreeNode::Internal(InternalNode::new(keys.clone(), children.clone(), next));
  page.serialize_from(&node).expect("serialize error");

  let d = match page.view::<BTreeNodeView>().expect("deserialize error") {
    BTreeNodeView::Internal(node) => node,
    BTreeNodeView::Leaf(_) => panic!("must be internal"),
  };

  for (i, c) in d.get_all_child().enumerate() {
    assert_eq!(children[i], c)
  }
}

#[test]
fn test_serialize_leaf() {
  let mut page = Page::new();

  let entries = vec![(vec![49, 50, 51], 100)];
  let next = Some(1100);

  let node = BTreeNode::Leaf(LeafNode::new(entries.clone(), next));
  page.serialize_from(&node).expect("serialize error");

  let d = page
    .view::<BTreeNodeView>()
    .expect("desiralize error")
    .as_leaf()
    .expect("desirialize leaf error");
  for (i, (k, p)) in d.get_entries().enumerate() {
    assert_eq!(entries[i].0, k.to_vec());
    assert_eq!(entries[i].1, p)
  }
  assert_eq!(d.get_next(), next)
}

#[test]
fn test_serialize_internal_with_keys_and_right() {
  let mut page = Page::new();
  let keys = vec![vec![1, 2], vec![3, 4]];
  let children = vec![10, 20, 30];
  let next = Some((99, vec![5, 6]));
  let node = BTreeNode::Internal(InternalNode::new(
    keys.clone(),
    children.clone(),
    next.clone(),
  ));
  page.serialize_from(&node).expect("serialize error");

  let d = match page.view::<BTreeNodeView>().expect("desiralize error") {
    BTreeNodeView::Internal(node) => node,
    BTreeNodeView::Leaf(_) => panic!("must be internal"),
  };

  assert_eq!(d.find(&vec![1, 1]).unwrap(), children[0]);
  assert_eq!(d.find(&vec![2, 2]).unwrap(), children[1]);
  assert_eq!(d.find(&vec![4, 4]).unwrap(), children[2]);
  assert_eq!(d.find(&vec![9, 9]).err(), Some(99));
}
