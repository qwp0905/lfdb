use crate::{
  disk::{Page, PageScanner, PageWriter, PAGE_SIZE},
  error::{Error, Result},
};

/**
 * Type tag written as the first byte of every serialized page.
 *
 * The tag prevents a page from being interpreted as the wrong object kind. This
 * catches invalid states such as dangling pointers, corrupted pages, or replay
 * mismatches before the remaining bytes are decoded as a different layout.
 */
#[derive(Debug)]
pub enum SerializeType {
  Header,
  BTreeNode,
  DataEntry,
}
impl SerializeType {
  const fn byte(&self) -> u8 {
    match self {
      Self::Header => 1,
      Self::BTreeNode => 2,
      Self::DataEntry => 3,
    }
  }
}

pub const SERIALIZABLE_BYTES: usize = PAGE_SIZE - 1; // 1 byte reserved for SerializeType Self

pub trait TypedObject {
  const TYPE: SerializeType;
}

/**
 * Owned page deserialization.
 *
 * Use this path when the caller needs to take ownership of the page contents,
 * usually because it will modify the object and serialize it back to a page.
 */
pub trait Deserializable: Sized + TypedObject {
  fn read_from(reader: &mut PageScanner) -> Result<Self>;
  fn deserialize(value: &Page<PAGE_SIZE>) -> Result<Self> {
    let mut reader = value.scanner();

    let expected = Self::TYPE.byte();
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
    writer.write(&[Self::TYPE.byte()])?;
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

/**
 * Borrowed zero-copy page view.
 *
 * Use this path for read-only access. The page type tag is checked, then the
 * returned view borrows byte ranges directly from the page instead of copying
 * them into an owned object.
 */
pub trait Viewable<'a>: Sized + TypedObject {
  fn view(page: &'a Page<PAGE_SIZE>) -> Result<Self> {
    let mut scanner = page.scanner();

    let expected = Self::TYPE.byte();
    let received = scanner.read()?;
    if expected != received {
      return Err(Error::DeserializeError(expected, received));
    }

    Self::read_from(page, &mut scanner)
  }

  fn read_from(page: &'a Page<PAGE_SIZE>, scanner: &mut PageScanner<'a>) -> Result<Self>;
}
