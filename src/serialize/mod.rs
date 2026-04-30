use crate::{
  disk::{Page, PageScanner, PageWriter, PAGE_SIZE},
  error::{Error, Result},
};

/**
 * A type tag written as the first byte of every serialized page.
 * Deserialization fails if the tag does not match, catching dangling
 * pointers or unreplayed WAL entries before corrupt data is read.
 */
#[derive(Debug)]
pub enum SerializeType {
  Header,
  BTreeNode,
  DataEntry,
  DataChunk,
}
impl SerializeType {
  const fn byte(&self) -> u8 {
    match self {
      SerializeType::Header => 1,
      SerializeType::BTreeNode => 2,
      SerializeType::DataEntry => 3,
      SerializeType::DataChunk => 4,
    }
  }
}

pub const SERIALIZABLE_BYTES: usize = PAGE_SIZE - 1; // 1 byte reserved for SerializeType tag

pub trait TypedObject {
  fn get_type() -> SerializeType;
}

pub trait Deserializable: Sized + TypedObject {
  fn read_from(reader: &mut PageScanner) -> Result<Self>;
  fn deserialize(value: &Page<PAGE_SIZE>) -> Result<Self> {
    let mut reader = value.scanner();

    let expected = Self::get_type().byte();
    let received = reader.read()?;
    if expected != received {
      return Err(Error::DeserializeError(expected, received));
    }

    Self::read_from(&mut reader)
  }
}

pub trait Serializable: Sized + TypedObject {
  fn serialize_at(&self, page: &mut Page<PAGE_SIZE>) -> Result<usize> {
    let mut writer = page.writer();
    writer.write(&[Self::get_type().byte()])?;
    self.write_at(&mut writer)?;
    Ok(writer.finalize())
  }
  fn write_at(&self, writer: &mut PageWriter) -> Result;
}
impl Page<PAGE_SIZE> {
  pub fn deserialize<T>(&self) -> Result<T>
  where
    T: Deserializable,
  {
    T::deserialize(self)
  }

  pub fn view<'a, T>(&'a self) -> Result<T>
  where
    T: Viewable<'a>,
  {
    T::view(self)
  }
}

pub trait SerializeFrom<T: Serializable> {
  fn serialize_from(&mut self, target: &T) -> Result<usize>;
}
impl<T: Serializable> SerializeFrom<T> for Page<PAGE_SIZE> {
  fn serialize_from(&mut self, target: &T) -> Result<usize> {
    target.serialize_at(self)
  }
}

pub trait Viewable<'a>: Sized + TypedObject {
  fn view(page: &'a Page<PAGE_SIZE>) -> Result<Self> {
    let mut scanner = page.scanner();

    let expected = Self::get_type().byte();
    let received = scanner.read()?;
    if expected != received {
      return Err(Error::DeserializeError(expected, received));
    }

    Self::read_from(page, &mut scanner)
  }

  fn read_from(page: &'a Page<PAGE_SIZE>, scanner: &mut PageScanner<'a>) -> Result<Self>;
}
