use crate::{
  disk::Page,
  objects::{RecordData, RecordDataView, TypedObject, VersionRecord},
};

use super::*;

#[test]
fn test_serialize_internal() {
  let keys = vec![];
  let children = vec![10];
  let next = None;
  let mut page = Page::new();
  let obj = TypedObject::from(BTreeNode::Internal(InternalNode::new(
    keys.clone(),
    children.clone(),
    next,
  )));
  page.serialize_from(&obj).expect("serialize error");

  let obj = page.view().unwrap();
  let d = obj.as_btree_node().unwrap().as_internal().unwrap();

  for (i, c) in d.get_all_child().enumerate() {
    assert_eq!(children[i], c)
  }
}

#[test]
fn test_serialize_leaf() {
  let mut page = Page::new();

  let mut obj = TypedObject::from(LeafNode::empty().to_node());
  let leaf = obj.as_btree_node_mut().unwrap().as_leaf_mut().unwrap();

  let entries = vec![(vec![49, 50, 51], (123, 456, vec![1, 2, 3]), 100)];

  for (i, (key, (o, v, d), p)) in entries.iter().enumerate() {
    leaf.insert_at(
      i,
      key.clone(),
      VersionRecord::new(*o, *v, RecordData::Data(d.clone())),
      *p,
    );
  }
  leaf.set_next(200);

  page.serialize_from(&obj).expect("serialize error");

  let obj = page.view().unwrap();
  let d = obj.as_btree_node().unwrap().as_leaf().unwrap();

  for (i, (s, e, r, ptr)) in d.get_entries().enumerate() {
    let (k, (o, v, d), p) = &entries[i];
    assert_eq!(page.range(s..e), k);
    assert_eq!(*p, ptr);
    assert_eq!(r.owner, *o);
    assert_eq!(r.version, *v);
    assert!(matches!(
      r.data,
      RecordDataView::Data(s, e) if d == page.range(s..e)
    ))
  }

  assert_eq!(d.get_next(), Some(200))
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
  let obj = TypedObject::from(node);
  page.serialize_from(&obj).expect("serialize error");

  let obj = page.view().unwrap();
  let d = obj.as_btree_node().unwrap().as_internal().unwrap();

  assert_eq!(d.find(&vec![1, 1]).unwrap(), children[0]);
  assert_eq!(d.find(&vec![2, 2]).unwrap(), children[1]);
  assert_eq!(d.find(&vec![4, 4]).unwrap(), children[2]);
  assert_eq!(d.find(&vec![9, 9]).err(), Some(99));
}
