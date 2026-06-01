// use crate::{
//   cursor::{leaf::*, record::*},
//   disk::Page,
//   serialize::SerializeFrom,
// };

// use super::*;

// #[test]
// fn test_serialize_internal() {
//   let keys = vec![];
//   let children = vec![10];
//   let next = None;
//   let mut page = Page::new();
//   let node = BTreeNode::Internal(InternalNode::new(keys.clone(), children.clone(), next));
//   page.serialize_from(&node).expect("serialize error");

//   let d = match page.view::<BTreeNodeView>().expect("deserialize error") {
//     BTreeNodeView::Internal(node) => node,
//     BTreeNodeView::Leaf(_) => panic!("must be internal"),
//   };

//   for (i, c) in d.get_all_child().enumerate() {
//     assert_eq!(children[i], c)
//   }
// }

// #[test]
// fn test_serialize_leaf() {
//   let mut page = Page::new();

//   let mut leaf = LeafNode::empty();

//   let entries = vec![(vec![49, 50, 51], (123, 456, vec![1, 2, 3]), 100)];

//   for (i, (key, (o, v, d), p)) in entries.iter().enumerate() {
//     leaf.insert_at(
//       i,
//       key.clone(),
//       VersionRecord::new(*o, *v, RecordData::Data(d.clone())),
//       *p,
//     );
//   }
//   leaf.set_next(200);

//   let node = BTreeNode::Leaf(leaf);
//   page.serialize_from(&node).expect("serialize error");

//   let d = page
//     .view::<BTreeNodeView>()
//     .expect("desiralize error")
//     .as_leaf()
//     .expect("desirialize leaf error");

//   for (i, (s, e, r, ptr)) in d.get_entries().enumerate() {
//     let (k, (o, v, d), p) = &entries[i];
//     assert_eq!(page.range(s..e), k);
//     assert_eq!(*p, ptr);
//     assert_eq!(r.owner, *o);
//     assert_eq!(r.version, *v);
//     assert!(matches!(
//       r.data,
//       RecordDataView::Data(s, e) if d == page.range(s..e)
//     ))
//   }

//   assert_eq!(d.get_next(), Some(200))
// }

// #[test]
// fn test_serialize_internal_with_keys_and_right() {
//   let mut page = Page::new();
//   let keys = vec![vec![1, 2], vec![3, 4]];
//   let children = vec![10, 20, 30];
//   let next = Some((99, vec![5, 6]));
//   let node = BTreeNode::Internal(InternalNode::new(
//     keys.clone(),
//     children.clone(),
//     next.clone(),
//   ));
//   page.serialize_from(&node).expect("serialize error");

//   let d = match page.view::<BTreeNodeView>().expect("desiralize error") {
//     BTreeNodeView::Internal(node) => node,
//     BTreeNodeView::Leaf(_) => panic!("must be internal"),
//   };

//   assert_eq!(d.find(&vec![1, 1]).unwrap(), children[0]);
//   assert_eq!(d.find(&vec![2, 2]).unwrap(), children[1]);
//   assert_eq!(d.find(&vec![4, 4]).unwrap(), children[2]);
//   assert_eq!(d.find(&vec![9, 9]).err(), Some(99));
// }
