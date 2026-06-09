use std::ops::Deref;

use crate::{
  disk::{PageRef, PAGE_SIZE},
  utils::SBox,
};

pub type StaticKey = Vec<u8>;
pub type StaticKeyRef<'a> = &'a [u8];

enum Type {
  Refed(SBox<PageRef<PAGE_SIZE>>, usize, usize),
  Copied(Vec<u8>),
}

pub struct VecRef(Type);
impl VecRef {
  pub const fn refed(page: SBox<PageRef<PAGE_SIZE>>, start: usize, end: usize) -> Self {
    Self(Type::Refed(page, start, end))
  }
  pub const fn copied(data: Vec<u8>) -> Self {
    Self(Type::Copied(data))
  }

  pub fn into_vec(self) -> Vec<u8> {
    match self.0 {
      Type::Refed(slot, s, e) => slot.copy_range(s..e),
      Type::Copied(data) => data,
    }
  }
}
impl Deref for VecRef {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    match &self.0 {
      Type::Refed(slot, s, e) => slot.range(*s..*e),
      Type::Copied(data) => data.as_slice(),
    }
  }
}
impl std::fmt::Debug for VecRef {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    self.deref().fmt(f)
  }
}
impl AsRef<[u8]> for VecRef {
  fn as_ref(&self) -> &[u8] {
    self.deref()
  }
}
impl PartialEq<[u8]> for VecRef {
  fn eq(&self, other: &[u8]) -> bool {
    self.deref().eq(other)
  }
}
impl<T: AsRef<[u8]>> PartialEq<T> for VecRef {
  fn eq(&self, other: &T) -> bool {
    self.eq(other.as_ref())
  }
}
impl Eq for VecRef {}

impl PartialOrd for VecRef {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.deref().cmp(other.deref()))
  }
}
impl Ord for VecRef {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self.deref().cmp(other.deref())
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
