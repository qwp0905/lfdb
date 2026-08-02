use super::super::*;
use super::*;
use crate::disk::Page;

#[test]
fn test_serialize_internal() {
  let keys = vec![];
  let children = vec![10];
  let next = None;
  let mut page = Page::new();
  let node = BTreeNode::Internal(InternalNode::new(
    keys.clone(),
    children.clone(),
    next,
    DEFAULT_BIAS,
  ));
  page.serialize_from(&node).expect("serialize error");

  let d = match page.view::<BTreeNodeView>().expect("deserialize error") {
    BTreeNodeView::Internal(node) => node,
    BTreeNodeView::Leaf(_) => panic!("must be internal"),
  };

  for (i, c) in d.get_all_child().unwrap().into_iter().enumerate() {
    assert_eq!(children[i], c)
  }
}

#[test]
fn test_serialize_leaf() {
  let mut page = Page::new();

  let mut leaf = LeafNode::empty();

  let entries = [(vec![49, 50, 51], (123, 456, vec![1, 2, 3]), 100)];

  for (i, (key, (o, v, d), _)) in entries.iter().enumerate() {
    leaf.insert_at(
      i,
      key.clone(),
      VersionRecord::new(*o, *v, RecordData::Data(d.clone())),
    );
  }
  leaf.set_next(vec![11, 2, 3, 3], 200);

  let node = BTreeNode::Leaf(leaf);
  page.serialize_from(&node).expect("serialize error");

  let d = page
    .view::<BTreeNodeView>()
    .expect("desiralize error")
    .into_leaf()
    .expect("desirialize leaf error");

  assert_eq!(d.top().unwrap(), &[49, 50, 51]);

  let mut iter = d.get_entries();
  let mut i = 0;
  while let Some(e) = iter.try_next().unwrap() {
    let (k, (o, v, d), _) = &entries[i];
    i += 1;
    assert_eq!(page.range(e.range), k);
    assert_eq!(e.record.owner, *o);
    assert_eq!(e.record.version, *v);
    assert!(matches!(
      e.record.data,
      RecordDataView::Data(range) if d == page.range(range.clone())
    ))
  }

  assert_eq!(d.get_next(), Some(200));
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
    DEFAULT_BIAS,
  ));
  page.serialize_from(&node).expect("serialize error");

  let d = match page.view::<BTreeNodeView>().expect("desiralize error") {
    BTreeNodeView::Internal(node) => node,
    BTreeNodeView::Leaf(_) => panic!("must be internal"),
  };

  assert_eq!(d.find(&[1, 1]).unwrap().unwrap(), children[0]);
  assert_eq!(d.find(&[2, 2]).unwrap().unwrap(), children[1]);
  assert_eq!(d.find(&[4, 4]).unwrap().unwrap(), children[2]);
  assert_eq!(d.find(&[9, 9]).unwrap().err(), Some(99));
}
