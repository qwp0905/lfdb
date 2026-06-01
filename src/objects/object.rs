use crate::{
  disk::{Page, PAGE_SIZE},
  Result,
};

use super::{
  BTreeNode, BTreeNodeView, DataChunk, DataChunkView, DataEntry, DataEntryView,
  SerializeType, TaggedObject, TreeHeader,
};

macro_rules! object_impl {
  ($pattern:tt, $as_ref:tt, $as_mut:tt, $owned_type:ty, $ref_type:ty, $life:lifetime $(,)?) => {
    impl TypedObject {
      pub fn $as_ref(&self) -> crate::Result<&$owned_type> {
        match self {
          Self::$pattern(v) => Ok(v),
          received => Err(crate::Error::DeserializeError(
            Some(<$owned_type>::TYPE),
            Some(received.get_type()),
          )),
        }
      }

      pub fn $as_mut(&mut self) -> crate::Result<&mut $owned_type> {
        match self {
          Self::$pattern(v) => Ok(v),
          received => Err(crate::Error::DeserializeError(
            Some(<$owned_type>::TYPE),
            Some(received.get_type()),
          )),
        }
      }
    }
    impl From<$owned_type> for TypedObject {
      fn from(value: $owned_type) -> Self {
        Self::$pattern(value)
      }
    }

    impl<$life> TypedObjectView<$life> {
      pub fn $as_ref(&$life self) -> crate::Result<&$life $ref_type> {
        match self {
          Self::$pattern(v) => Ok(v),
           received => Err(crate::Error::DeserializeError(
            Some(<$owned_type>::TYPE),
            Some(received.get_type()),
          )),
        }
      }
    }
  };
}

pub enum TypedObject {
  Header(TreeHeader),
  BTreeNode(BTreeNode),
  DataEntry(DataEntry),
  DataChunk(DataChunk),
}
object_impl! {
  Header,
  as_tree_header,
  as_tree_header_mut,
  TreeHeader,
  TreeHeader,
  'a,
}
object_impl! {
  BTreeNode,
  as_btree_node,
  as_btree_node_mut,
  BTreeNode,
  BTreeNodeView<'a>,
  'a,
}
object_impl! {
  DataEntry,
  as_data_entry,
  as_data_entry_mut,
  DataEntry,
  DataEntryView,
  'a,
}
object_impl! {
  DataChunk,
  as_data_chunk,
  as_data_chunk_mut,
  DataChunk,
  DataChunkView<'a>,
  'a,
}
impl TypedObject {
  fn deserialize_from(page: &Page<PAGE_SIZE>) -> Result<Self> {
    let mut reader = page.scanner();
    match reader.read().and_then(SerializeType::deserialize_byte)? {
      SerializeType::Header => TreeHeader::read_from(&mut reader).map(Self::Header),
      SerializeType::BTreeNode => BTreeNode::read_from(&mut reader).map(Self::BTreeNode),
      SerializeType::DataEntry => DataEntry::read_from(&mut reader).map(Self::DataEntry),
      SerializeType::DataChunk => DataChunk::read_from(&mut reader).map(Self::DataChunk),
    }
  }

  fn get_type(&self) -> SerializeType {
    match self {
      Self::Header(_) => SerializeType::Header,
      Self::BTreeNode(_) => SerializeType::BTreeNode,
      Self::DataEntry(_) => SerializeType::DataEntry,
      Self::DataChunk(_) => SerializeType::DataChunk,
    }
  }

  fn serialize_at(&self, page: &mut Page<PAGE_SIZE>) -> Result<usize> {
    let mut writer = page.writer();
    match self {
      Self::Header(data) => {
        writer.write(&[TreeHeader::TYPE.type_byte()])?;
        data.write_at(&mut writer)?;
      }
      Self::BTreeNode(data) => {
        writer.write(&[BTreeNode::TYPE.type_byte()])?;
        data.write_at(&mut writer)?;
      }
      Self::DataEntry(data) => {
        writer.write(&[DataEntry::TYPE.type_byte()])?;
        data.write_at(&mut writer)?;
      }
      Self::DataChunk(data) => {
        writer.write(&[DataChunk::TYPE.type_byte()])?;
        data.write_at(&mut writer)?;
      }
    }
    Ok(writer.finalize())
  }
}

pub enum TypedObjectView<'a> {
  Header(TreeHeader),
  BTreeNode(BTreeNodeView<'a>),
  DataEntry(DataEntryView),
  DataChunk(DataChunkView<'a>),
}
impl<'a> TypedObjectView<'a> {
  fn deserialize_from(page: &'a Page<PAGE_SIZE>) -> Result<Self> {
    let mut reader = page.scanner();
    match reader.read().and_then(SerializeType::deserialize_byte)? {
      SerializeType::Header => TreeHeader::read_from(&mut reader).map(Self::Header),
      SerializeType::BTreeNode => {
        BTreeNodeView::read_from(page, &mut reader).map(Self::BTreeNode)
      }
      SerializeType::DataEntry => {
        DataEntryView::read_from(&mut reader).map(Self::DataEntry)
      }
      SerializeType::DataChunk => {
        DataChunkView::read_from(page, &mut reader).map(Self::DataChunk)
      }
    }
  }

  fn get_type(&self) -> SerializeType {
    match self {
      Self::Header(_) => SerializeType::Header,
      Self::BTreeNode(_) => SerializeType::BTreeNode,
      Self::DataEntry(_) => SerializeType::DataEntry,
      Self::DataChunk(_) => SerializeType::DataChunk,
    }
  }
}

impl Page<PAGE_SIZE> {
  pub fn view(&self) -> Result<TypedObjectView<'_>> {
    TypedObjectView::deserialize_from(self)
  }
  pub fn deserialize(&self) -> Result<TypedObject> {
    TypedObject::deserialize_from(self)
  }
  pub fn serialize_from(&mut self, object: &TypedObject) -> Result<usize> {
    object.serialize_at(self)
  }
}
