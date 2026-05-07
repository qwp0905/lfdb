use std::{
  fmt::Debug,
  marker::PhantomData,
  mem::MaybeUninit,
  ops::{Deref, DerefMut, Index, IndexMut},
  ptr::copy_nonoverlapping,
  slice::{from_raw_parts, from_raw_parts_mut},
  vec::IntoIter,
};

#[macro_export]
macro_rules! inline_vec {
  () => {
    $crate::utils::InlineVec::new()
  };
  ($elem:expr; $n:expr) => {
    $crate::utils::InlineVec::from_elem($elem, $n)
  };
  ($($x:expr),+ $(,)?) => {
    $crate::utils::InlineVec::from([$($x),+].as_slice())
  };
}

struct Array<T, const N: usize> {
  data: [MaybeUninit<T>; N],
  len: usize,
}
impl<T, const N: usize> Array<T, N> {
  const fn new() -> Self {
    Self {
      data: [const { MaybeUninit::uninit() }; N],
      len: 0,
    }
  }
  fn push(&mut self, value: T) -> std::result::Result<(), T> {
    if self.len >= N {
      return Err(value);
    }
    self.data[self.len].write(value);
    self.len += 1;
    Ok(())
  }
  fn pop(&mut self) -> Option<T> {
    if self.len == 0 {
      return None;
    }
    self.len -= 1;
    Some(unsafe { self.data[self.len].assume_init_read() })
  }
  #[inline]
  const fn as_ptr(&self) -> *const T {
    self.data.as_ptr() as _
  }
  #[inline]
  const fn as_mut_ptr(&mut self) -> *mut T {
    self.data.as_mut_ptr() as _
  }
  #[inline]
  const fn as_slice(&self) -> &[T] {
    unsafe { from_raw_parts(self.as_ptr(), self.len) }
  }
  #[inline]
  const fn as_mut_slice(&mut self) -> &mut [T] {
    unsafe { from_raw_parts_mut(self.as_mut_ptr(), self.len) }
  }
  #[inline]
  const fn len(&self) -> usize {
    self.len
  }
  fn into_vec(mut self) -> Vec<T> {
    let mut vector = Vec::with_capacity(self.len);
    unsafe { vector.set_len(self.len) };
    unsafe { copy_nonoverlapping(self.as_ptr(), vector.as_mut_ptr(), self.len) };
    self.len = 0;
    vector
  }
}
impl<T, const N: usize> Drop for Array<T, N> {
  fn drop(&mut self) {
    for i in 0..self.len {
      unsafe { self.data[i].assume_init_drop() };
    }
  }
}
impl<T: Clone, const N: usize> Clone for Array<T, N> {
  fn clone(&self) -> Self {
    let mut data = [const { MaybeUninit::uninit() }; N];
    for i in 0..self.len {
      data[i].write(unsafe { self.data[i].assume_init_ref() }.clone());
    }
    Self {
      data,
      len: self.len,
    }
  }
}
impl<T, const N: usize> IntoIterator for Array<T, N> {
  type Item = T;

  type IntoIter = ArrayIntoIter<T, N>;

  fn into_iter(mut self) -> Self::IntoIter {
    let mut data = [const { MaybeUninit::uninit() }; N];
    let len = self.len;
    unsafe { copy_nonoverlapping(self.as_ptr(), data.as_mut_ptr() as *mut T, self.len) };
    self.len = 0;
    Self::IntoIter {
      data,
      len,
      current: 0,
    }
  }
}

pub struct ArrayIntoIter<T, const N: usize> {
  data: [MaybeUninit<T>; N],
  len: usize,
  current: usize,
}
impl<T, const N: usize> Iterator for ArrayIntoIter<T, N> {
  type Item = T;

  fn next(&mut self) -> Option<Self::Item> {
    if self.len == self.current {
      return None;
    }
    let value = unsafe { self.data[self.current].assume_init_read() };
    self.current += 1;
    Some(value)
  }
}
impl<T, const N: usize> Drop for ArrayIntoIter<T, N> {
  fn drop(&mut self) {
    for i in self.current..self.len {
      unsafe { self.data[i].assume_init_drop() };
    }
  }
}

enum Type<T, const N: usize> {
  Inline(Array<T, N>),
  Heap(Vec<T>),
}

pub struct InlineVec<T, const N: usize>(Type<T, N>);
impl<T, const N: usize> InlineVec<T, N> {
  #[doc(hidden)]
  #[allow(unused)]
  pub fn from_elem(elem: T, n: usize) -> Self
  where
    T: Clone,
  {
    if n > N {
      return Self(Type::Heap(vec![elem; n]));
    }
    let mut array = Array::new();
    let ptr: *mut T = array.as_mut_ptr();
    for i in 0..(n - 1) {
      unsafe { ptr.add(i).write(elem.clone()) };
    }
    if n > 0 {
      unsafe { ptr.add(n - 1).write(elem) };
    }
    array.len = n;
    Self(Type::Inline(array))
  }

  pub fn new() -> Self {
    Self(Type::Inline(Array::new()))
  }

  pub fn with_capacity(capacity: usize) -> Self {
    if capacity > N {
      Self(Type::Heap(Vec::with_capacity(capacity)))
    } else {
      Self::new()
    }
  }

  pub fn push(&mut self, value: T) {
    let (array, value) = match &mut self.0 {
      Type::Inline(array) => match array.push(value) {
        Ok(_) => return,
        Err(value) => (array, value),
      },
      Type::Heap(vector) => return vector.push(value),
    };

    let mut grown = Vec::with_capacity(array.len << 1);
    unsafe { grown.set_len(array.len + 1) };
    unsafe { copy_nonoverlapping(array.as_ptr(), grown.as_mut_ptr(), array.len()) };
    unsafe { grown.as_mut_ptr().add(array.len).write(value) };
    array.len = 0;
    self.0 = Type::Heap(grown)
  }
  pub fn pop(&mut self) -> Option<T> {
    match &mut self.0 {
      Type::Inline(array) => array.pop(),
      Type::Heap(vector) => vector.pop(),
    }
  }
  pub const fn len(&self) -> usize {
    match &self.0 {
      Type::Inline(array) => array.len(),
      Type::Heap(vector) => vector.len(),
    }
  }
  pub const fn as_ptr(&self) -> *const T {
    match &self.0 {
      Type::Inline(array) => array.as_ptr(),
      Type::Heap(vector) => vector.as_ptr(),
    }
  }
  pub const fn as_mut_ptr(&mut self) -> *mut T {
    match &mut self.0 {
      Type::Inline(array) => array.as_mut_ptr(),
      Type::Heap(vector) => vector.as_mut_ptr(),
    }
  }
  pub fn as_slice(&self) -> &[T] {
    match &self.0 {
      Type::Inline(array) => array.as_slice(),
      Type::Heap(vector) => vector.as_slice(),
    }
  }

  pub fn iter(&self) -> InlineVecIter<'_, T, N> {
    InlineVecIter {
      ptr: self.as_ptr(),
      size: self.len(),
      current: 0,
      _marker: PhantomData,
    }
  }
}
impl<T, const N: usize> Deref for InlineVec<T, N> {
  type Target = [T];

  fn deref(&self) -> &Self::Target {
    self.as_slice()
  }
}
impl<T, const N: usize> DerefMut for InlineVec<T, N> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    match &mut self.0 {
      Type::Inline(array) => array.as_mut_slice(),
      Type::Heap(vector) => vector.as_mut_slice(),
    }
  }
}
impl<T, const N: usize> Index<usize> for InlineVec<T, N> {
  type Output = T;

  fn index(&self, index: usize) -> &Self::Output {
    assert!(index < self.len());
    unsafe { &*self.as_ptr().add(index) }
  }
}
impl<T, const N: usize> IndexMut<usize> for InlineVec<T, N> {
  fn index_mut(&mut self, index: usize) -> &mut Self::Output {
    assert!(index < self.len());
    unsafe { &mut *self.as_mut_ptr().add(index) }
  }
}
impl<T, const N: usize> Index<std::ops::Range<usize>> for InlineVec<T, N> {
  type Output = [T];

  fn index(&self, index: std::ops::Range<usize>) -> &Self::Output {
    assert!(index.start <= index.end);
    let len = self.len();
    assert!(index.start < len);
    assert!(index.end <= len);
    unsafe { from_raw_parts(self.as_ptr().add(index.start), index.end - index.start) }
  }
}
impl<T, const N: usize> IndexMut<std::ops::Range<usize>> for InlineVec<T, N> {
  fn index_mut(&mut self, index: std::ops::Range<usize>) -> &mut Self::Output {
    assert!(index.start <= index.end);
    let len = self.len();
    assert!(index.start < len);
    assert!(index.end <= len);
    unsafe {
      from_raw_parts_mut(self.as_mut_ptr().add(index.start), index.end - index.start)
    }
  }
}
impl<T, const N: usize> From<Vec<T>> for InlineVec<T, N> {
  fn from(value: Vec<T>) -> Self {
    Self(Type::Heap(value))
  }
}
impl<T, const N: usize> From<InlineVec<T, N>> for Vec<T> {
  fn from(value: InlineVec<T, N>) -> Self {
    match value.0 {
      Type::Inline(array) => array.into_vec(),
      Type::Heap(vector) => vector,
    }
  }
}
impl<T: PartialEq, const N: usize> PartialEq for InlineVec<T, N> {
  fn eq(&self, other: &Self) -> bool {
    self.as_slice().eq(other.as_slice())
  }
}
impl<T: Eq, const N: usize> Eq for InlineVec<T, N> {}
impl<T: PartialOrd, const N: usize> PartialOrd for InlineVec<T, N> {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    self.as_slice().partial_cmp(other.as_slice())
  }
}
impl<T: Ord, const N: usize> Ord for InlineVec<T, N> {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self.as_slice().cmp(other.as_slice())
  }
}
impl<T: Copy, const N: usize> From<&[T]> for InlineVec<T, N> {
  fn from(value: &[T]) -> Self {
    if value.len() > N {
      return Self(Type::Heap(value.to_vec()));
    }
    let mut array = Array::new();
    unsafe { copy_nonoverlapping(value.as_ptr(), array.as_mut_ptr(), value.len()) };
    array.len = value.len();
    Self(Type::Inline(array))
  }
}
impl<T: Debug, const N: usize> Debug for InlineVec<T, N> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    self.deref().fmt(f)
  }
}
impl<T: Clone, const N: usize> Clone for InlineVec<T, N> {
  fn clone(&self) -> Self {
    match &self.0 {
      Type::Inline(array) => Self(Type::Inline(Clone::clone(array))),
      Type::Heap(vector) => Self(Type::Heap(Clone::clone(vector))),
    }
  }
}

impl<T, const N: usize> IntoIterator for InlineVec<T, N> {
  type Item = T;

  type IntoIter = InlineVecIntoIter<T, N>;

  fn into_iter(self) -> Self::IntoIter {
    match self.0 {
      Type::Inline(array) => InlineVecIntoIter::Inline(array.into_iter()),
      Type::Heap(vector) => InlineVecIntoIter::Heap(vector.into_iter()),
    }
  }
}
pub enum InlineVecIntoIter<T, const N: usize> {
  Inline(ArrayIntoIter<T, N>),
  Heap(IntoIter<T>),
}
impl<T, const N: usize> Iterator for InlineVecIntoIter<T, N> {
  type Item = T;

  fn next(&mut self) -> Option<Self::Item> {
    match self {
      InlineVecIntoIter::Inline(iter) => iter.next(),
      InlineVecIntoIter::Heap(iter) => iter.next(),
    }
  }
}
pub struct InlineVecIter<'a, T, const N: usize> {
  ptr: *const T,
  size: usize,
  current: usize,
  _marker: PhantomData<&'a InlineVec<T, N>>,
}
impl<'a, T, const N: usize> Iterator for InlineVecIter<'a, T, N> {
  type Item = &'a T;

  fn next(&mut self) -> Option<Self::Item> {
    if self.current == self.size {
      return None;
    }

    let value = unsafe { &*self.ptr.add(self.current) };
    self.current += 1;
    Some(value)
  }
}
impl<'a, T, const N: usize> IntoIterator for &'a InlineVec<T, N> {
  type Item = &'a T;

  type IntoIter = InlineVecIter<'a, T, N>;

  fn into_iter(self) -> Self::IntoIter {
    self.iter()
  }
}

#[cfg(test)]
#[path = "tests/inline_vec.rs"]
mod tests;
