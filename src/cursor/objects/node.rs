use super::{InternalNode, InternalNodeView, LeafNode, LeafNodeView};
use crate::{
  disk::{PageScanner, PageWriter},
  serialize::{Deserializable, Serializable, SerializeType, TypedObject, Viewable},
  Error, Result,
};

/**
 * Borrowed view of a serialized B-tree node.
 *
 * A B-tree page first has the outer `SerializeType::BTreeNode` tag, then this
 * node-level tag selecting internal or leaf layout. This view reads that nested
 * type without materializing the node.
 */
pub enum BTreeNodeView<'a> {
  Internal(InternalNodeView<'a>),
  Leaf(LeafNodeView<'a>),
}
impl<'a> TypedObject for BTreeNodeView<'a> {
  const TYPE: SerializeType = BTreeNode::TYPE;
}

impl<'a> Viewable<'a> for BTreeNodeView<'a> {
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

impl<'a> BTreeNodeView<'a> {
  #[inline]
  pub fn into_leaf(self) -> Result<LeafNodeView<'a>> {
    match self {
      Self::Internal(_) => Err(Error::InvalidFormat("invalid leaf node type")),
      Self::Leaf(node) => Ok(node),
    }
  }
  #[inline]
  pub fn into_internal(self) -> Result<InternalNodeView<'a>> {
    match self {
      Self::Internal(node) => Ok(node),
      Self::Leaf(_) => Err(Error::InvalidFormat("invalid internal node type")),
    }
  }
}

/**
 * Owned B-tree node.
 *
 * This is the owned counterpart of `BTreeNodeView`: a typed object whose payload
 * contains another small type tag for internal-vs-leaf node layout.
 */
#[derive(Debug)]
pub enum BTreeNode {
  Internal(InternalNode),
  Leaf(LeafNode),
}
impl BTreeNode {
  pub const fn initial_state() -> Self {
    Self::Leaf(LeafNode::empty())
  }
  pub fn into_internal(self) -> Result<InternalNode> {
    match self {
      Self::Internal(node) => Ok(node),
      Self::Leaf(_) => Err(Error::InvalidFormat("invalid internal node type")),
    }
  }
  pub fn into_leaf(self) -> Result<LeafNode> {
    match self {
      Self::Internal(_) => Err(Error::InvalidFormat("invalid leaf node type")),
      Self::Leaf(node) => Ok(node),
    }
  }
}
impl TypedObject for BTreeNode {
  const TYPE: SerializeType = SerializeType::BTreeNode;
}
impl Serializable for BTreeNode {
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
}
impl Deserializable for BTreeNode {
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
  pub const fn into_node(self) -> BTreeNode {
    BTreeNode::Leaf(self)
  }
}
impl InternalNode {
  #[inline]
  pub const fn into_node(self) -> BTreeNode {
    BTreeNode::Internal(self)
  }
}

#[cfg(test)]
#[path = "tests/node.rs"]
mod tests;
