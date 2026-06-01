use super::{
  InternalNode, InternalNodeView, LeafNode, LeafNodeView, SerializeType, TaggedObject,
};
use crate::{
  disk::{PageScanner, PageWriter},
  Error, Result,
};

pub enum BTreeNodeView<'a> {
  Internal(InternalNodeView<'a>),
  Leaf(LeafNodeView<'a>),
}
impl<'a> BTreeNodeView<'a> {
  #[inline]
  pub fn as_leaf(&self) -> Result<&LeafNodeView<'a>> {
    match self {
      Self::Internal(_) => Err(Error::InvalidFormat("invalid leaf node type")),
      Self::Leaf(node) => Ok(node),
    }
  }
  #[inline]
  pub fn as_internal(&self) -> Result<&InternalNodeView<'a>> {
    match self {
      Self::Internal(node) => Ok(node),
      Self::Leaf(_) => Err(Error::InvalidFormat("invalid internal node type")),
    }
  }

  pub fn read_from(
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
#[derive(Debug)]
pub enum BTreeNode {
  Internal(InternalNode),
  Leaf(LeafNode),
}
impl BTreeNode {
  pub const fn initial_state() -> Self {
    Self::Leaf(LeafNode::empty())
  }
  pub fn as_internal_mut(&mut self) -> Result<&mut InternalNode> {
    match self {
      Self::Internal(node) => Ok(node),
      Self::Leaf(_) => Err(Error::InvalidFormat("invalid internal node type")),
    }
  }
  pub fn as_leaf_mut(&mut self) -> Result<&mut LeafNode> {
    match self {
      Self::Internal(_) => Err(Error::InvalidFormat("invalid leaf node type")),
      Self::Leaf(node) => Ok(node),
    }
  }
  pub fn write_at(&self, writer: &mut PageWriter) -> Result {
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
  pub fn read_from(scanner: &mut PageScanner) -> Result<Self> {
    match scanner.read()? {
      0 => Ok(Self::Internal(InternalNode::from_scanner(scanner)?)),
      1 => Ok(Self::Leaf(LeafNode::from_scanner(scanner)?)),
      _ => Err(Error::InvalidFormat("invalid cursor node type")),
    }
  }
}

impl TaggedObject for BTreeNode {
  const TYPE: SerializeType = SerializeType::BTreeNode;
}
impl<'a> TaggedObject for BTreeNodeView<'a> {
  const TYPE: SerializeType = BTreeNode::TYPE;
}

impl LeafNode {
  #[inline]
  pub const fn to_node(self) -> BTreeNode {
    BTreeNode::Leaf(self)
  }
}
impl InternalNode {
  #[inline]
  pub const fn to_node(self) -> BTreeNode {
    BTreeNode::Internal(self)
  }
}

#[cfg(test)]
#[path = "tests/node.rs"]
mod tests;
