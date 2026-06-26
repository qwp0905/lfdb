use std::{borrow, cmp, fmt, hash, ops::Deref};

use crate::{
  disk::{AlignedBuf, PageRef, PAGE_SIZE},
  utils::SBox,
};

pub type StaticKey = Vec<u8>;
pub type StaticKeyRef<'a> = &'a [u8];

enum Type {
  Refed(SBox<PageRef<PAGE_SIZE>>, usize, usize),
  Copied(AlignedBuf),
}

/**
 * Byte-vector view used by cursor reads.
 *
 * `VecRef` avoids copying when the bytes already live inside a cached page: it
 * keeps an `SBox` reference to the page and exposes the requested range as a
 * slice. When bytes must be materialized elsewhere, it stores the copied aligned
 * buffer but presents the same `[u8]` interface.
 */
pub struct VecRef(Type);
impl VecRef {
  pub const fn refed(page: SBox<PageRef<PAGE_SIZE>>, start: usize, end: usize) -> Self {
    Self(Type::Refed(page, start, end))
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
      Type::Refed(slot, s, e) => slot.copy_range(s..e),
      Type::Copied(data) => data.as_slice().to_vec(),
    }
  }
  fn as_slice(&self) -> &[u8] {
    match &self.0 {
      Type::Refed(slot, s, e) => slot.range(*s..*e),
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
      Type::Refed(page, s, e) => Self::refed(page.clone(), *s, *e),
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
