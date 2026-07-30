use std::{
  borrow, cmp, fmt, hash,
  ops::{Deref, Range},
};

use crate::{cache::ReadonlySlot, disk::AlignedBuf};

enum Type {
  Refed(ReadonlySlot, Range<usize>),
  Copied(AlignedBuf),
}

/**
 * Byte-vector view used by cursor reads.
 *
 * `VecRef` avoids copying when the bytes already live inside a cached page: it
 * keeps an `ReadonlySlot` reference to the page and exposes the requested range as a
 * slice. When bytes must be materialized elsewhere, it stores the copied aligned
 * buffer but presents the same `[u8]` interface.
 */
pub struct VecRef(Type);
impl VecRef {
  pub const fn refed(page: ReadonlySlot, range: Range<usize>) -> Self {
    Self(Type::Refed(page, range))
  }
  pub const fn copied(data: AlignedBuf) -> Self {
    Self(Type::Copied(data))
  }

  /**
   * Materialize this view as owned bytes.
   *
   * Use this only when the caller needs an owned `Vec`; otherwise `VecRef` can be
   * read directly as a byte slice.
   */
  pub fn into_vec(self) -> Vec<u8> {
    match self.0 {
      Type::Refed(slot, range) => slot.as_ref().copy_range(range),
      Type::Copied(data) => data.as_slice().to_vec(),
    }
  }
  fn as_slice(&self) -> &[u8] {
    match &self.0 {
      Type::Refed(slot, range) => slot.as_ref().range(range.clone()),
      Type::Copied(data) => data.as_slice(),
    }
  }
}
impl Deref for VecRef {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    self.as_slice()
  }
}
impl fmt::Debug for VecRef {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    self.as_slice().fmt(f)
  }
}
impl AsRef<[u8]> for VecRef {
  fn as_ref(&self) -> &[u8] {
    self.as_slice()
  }
}
impl PartialEq<[u8]> for VecRef {
  fn eq(&self, other: &[u8]) -> bool {
    self.as_slice().eq(other)
  }
}
impl<T: AsRef<[u8]>> PartialEq<T> for VecRef {
  fn eq(&self, other: &T) -> bool {
    self.eq(other.as_ref())
  }
}
impl Eq for VecRef {}

impl PartialOrd for VecRef {
  fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
    Some(self.cmp(other))
  }
}
impl Ord for VecRef {
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    self.as_slice().cmp(other.deref())
  }
}
impl Clone for VecRef {
  fn clone(&self) -> Self {
    match &self.0 {
      Type::Refed(page, range) => Self::refed(page.clone(), range.clone()),
      Type::Copied(data) => Self::copied(data.clone()),
    }
  }
}
impl borrow::Borrow<[u8]> for VecRef {
  fn borrow(&self) -> &[u8] {
    self.as_slice()
  }
}
impl hash::Hash for VecRef {
  fn hash<H: hash::Hasher>(&self, state: &mut H) {
    self.as_slice().hash(state)
  }
}
