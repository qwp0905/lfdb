use super::{InternalNode, InternalNodeView, LeafNode, LeafNodeView};
use crate::{
  disk::{PageScanner, PageWriter},
  serialize::{Serializable, SerializeType, Viewable},
  Error, Result,
};

pub enum CursorNodeView<'a> {
  Internal(InternalNodeView<'a>),
  Leaf(LeafNodeView<'a>),
}
impl<'a> Viewable<'a> for CursorNodeView<'a> {
  fn get_type() -> SerializeType {
    SerializeType::CursorNode
  }

  fn read_from(
    page: &'a crate::disk::Page,
    scanner: &mut PageScanner<'a>,
  ) -> Result<Self> {
    match scanner.read()? {
      0 => Ok(Self::Internal(InternalNodeView::from_scanner(
        page, scanner,
      )?)),
      1 => Ok(Self::Leaf(LeafNodeView::from_scanner(page, scanner)?)),
      _ => Err(Error::InvalidFormat("invalid cursor node type")),
    }
  }
}

impl<'a> CursorNodeView<'a> {
  pub fn as_leaf(self) -> Result<LeafNodeView<'a>> {
    match self {
      Self::Internal(_) => Err(Error::InvalidFormat("invalid leaf node type")),
      Self::Leaf(node) => Ok(node),
    }
  }
  // pub fn as_internal(self) -> Result<InternalNodeView<'a>> {
  //   match self {
  //     Self::Internal(node) => Ok(node),
  //     Self::Leaf(_) => Err(Error::InvalidFormat("invalid internal node type")),
  //   }
  // }
}
#[derive(Debug)]
pub enum CursorNode {
  Internal(InternalNode),
  Leaf(LeafNode),
}
impl CursorNode {
  pub fn initial_state() -> Self {
    Self::Leaf(LeafNode::new(Default::default(), None))
  }
  pub fn as_leaf(self) -> Result<LeafNode> {
    match self {
      Self::Internal(_) => Err(Error::InvalidFormat("invalid leaf node type")),
      Self::Leaf(node) => Ok(node),
    }
  }
  pub fn as_internal(self) -> Result<InternalNode> {
    match self {
      Self::Internal(node) => Ok(node),
      Self::Leaf(_) => Err(Error::InvalidFormat("invalid internal node type")),
    }
  }
}
impl Serializable for CursorNode {
  fn get_type() -> SerializeType {
    SerializeType::CursorNode
  }
  fn write_at(&self, writer: &mut PageWriter) -> Result {
    match self {
      Self::Internal(node) => {
        writer.write(&[0])?;
        node.write_at(writer)?;
      }
      Self::Leaf(node) => {
        writer.write(&[1])?;
        node.write_at(writer)?;
      }
    }
    Ok(())
  }

  fn read_from(scanner: &mut PageScanner) -> Result<Self> {
    match scanner.read()? {
      0 => Ok(Self::Internal(InternalNode::from_scanner(scanner)?)),
      1 => Ok(Self::Leaf(LeafNode::from_scanner(scanner)?)),
      _ => Err(Error::InvalidFormat("invalid cursor node type")),
    }
  }
}

impl LeafNode {
  #[inline]
  pub fn to_node(self) -> CursorNode {
    CursorNode::Leaf(self)
  }
}
impl InternalNode {
  #[inline]
  pub fn to_node(self) -> CursorNode {
    CursorNode::Internal(self)
  }
}
