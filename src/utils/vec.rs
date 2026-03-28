use std::{
  mem::MaybeUninit,
  ops::{Index, IndexMut},
  ptr::copy_nonoverlapping,
};

use crate::utils::{ToRawPointer, UnsafeBorrow, UnsafeBorrowMut, UnsafeDrop};

enum Writable<'a, T> {
  Vec(&'a mut Vec<T>),
  Array(&'a mut [MaybeUninit<T>]),
}
enum Readable<'a, T> {
  Vec(&'a Vec<T>),
  Array(&'a [MaybeUninit<T>]),
}

#[inline(always)]
fn uninit<const N: usize, T>() -> [MaybeUninit<T>; N] {
  [const { MaybeUninit::uninit() }; N]
}

enum Slot<T> {
  EMPTY,
  S1([MaybeUninit<T>; 1]),
  S2([MaybeUninit<T>; 2]),
  S4([MaybeUninit<T>; 4]),
  S8([MaybeUninit<T>; 8]),
  S16([MaybeUninit<T>; 16]),
  S32([MaybeUninit<T>; 32]),
  S64([MaybeUninit<T>; 64]),
  S128([MaybeUninit<T>; 128]),
  S256([MaybeUninit<T>; 256]),
  Heap(*mut Vec<T>),
}
impl<T> Slot<T> {
  fn from_cap(cap: usize) -> Self {
    if cap == 0 {
      return Self::EMPTY;
    }
    if cap <= 1 {
      return Self::S1(uninit());
    }
    if cap <= 2 {
      return Self::S2(uninit());
    }
    if cap <= 4 {
      return Self::S4(uninit());
    }
    if cap <= 8 {
      return Self::S8(uninit());
    }
    if cap <= 16 {
      return Self::S16(uninit());
    }
    if cap <= 32 {
      return Self::S32(uninit());
    }
    if cap <= 64 {
      return Self::S64(uninit());
    }
    if cap <= 128 {
      return Self::S128(uninit());
    }
    if cap <= 256 {
      return Self::S256(uninit());
    }
    Self::Heap(Vec::<T>::with_capacity(cap).to_raw_ptr() as *mut _)
  }
  #[inline(always)]
  fn capacity(&self) -> usize {
    match self {
      Self::EMPTY => 0,
      Self::S1(_) => 1,
      Self::S2(_) => 2,
      Self::S4(_) => 4,
      Self::S8(_) => 8,
      Self::S16(_) => 16,
      Self::S32(_) => 32,
      Self::S64(_) => 64,
      Self::S128(_) => 128,
      Self::S256(_) => 256,
      Self::Heap(vec) => vec.borrow_unsafe().capacity(),
    }
  }

  #[inline(never)]
  fn grow(&self) -> Self {
    match self {
      Self::EMPTY => Self::S1(uninit()),
      Self::S1(old) => {
        let mut arr = uninit();
        unsafe { copy_nonoverlapping(old.as_ptr(), arr.as_mut_ptr(), old.len()) };
        Self::S2(arr)
      }
      Self::S2(old) => {
        let mut arr = uninit();
        unsafe { copy_nonoverlapping(old.as_ptr(), arr.as_mut_ptr(), old.len()) };
        Self::S4(arr)
      }
      Self::S4(old) => {
        let mut arr = uninit();
        unsafe { copy_nonoverlapping(old.as_ptr(), arr.as_mut_ptr(), old.len()) };
        Self::S8(arr)
      }
      Self::S8(old) => {
        let mut arr = uninit();
        unsafe { copy_nonoverlapping(old.as_ptr(), arr.as_mut_ptr(), old.len()) };
        Self::S16(arr)
      }
      Self::S16(old) => {
        let mut arr = uninit();
        unsafe { copy_nonoverlapping(old.as_ptr(), arr.as_mut_ptr(), old.len()) };
        Self::S32(arr)
      }
      Self::S32(old) => {
        let mut arr = uninit();
        unsafe { copy_nonoverlapping(old.as_ptr(), arr.as_mut_ptr(), old.len()) };
        Self::S64(arr)
      }
      Self::S64(old) => {
        let mut arr = uninit();
        unsafe { copy_nonoverlapping(old.as_ptr(), arr.as_mut_ptr(), old.len()) };
        Self::S128(arr)
      }
      Self::S128(old) => {
        let mut arr = uninit();
        unsafe { copy_nonoverlapping(old.as_ptr(), arr.as_mut_ptr(), old.len()) };
        Self::S256(arr)
      }
      Self::S256(old) => {
        let len = old.len();
        let mut vec = Vec::with_capacity(len);
        unsafe { vec.set_len(len) };
        unsafe { copy_nonoverlapping(old.as_ptr(), vec.as_mut_ptr(), len) };
        Self::Heap(vec.to_raw_ptr() as *mut _)
      }
      Self::Heap(slot) => Self::Heap(*slot),
    }
  }

  #[inline]
  fn readable(&self) -> Readable<'_, T> {
    match self {
      Slot::EMPTY => unreachable!(),
      Slot::S1(slot) => Readable::Array(slot),
      Slot::S2(slot) => Readable::Array(slot),
      Slot::S4(slot) => Readable::Array(slot),
      Slot::S8(slot) => Readable::Array(slot),
      Slot::S16(slot) => Readable::Array(slot),
      Slot::S32(slot) => Readable::Array(slot),
      Slot::S64(slot) => Readable::Array(slot),
      Slot::S128(slot) => Readable::Array(slot),
      Slot::S256(slot) => Readable::Array(slot),
      Slot::Heap(slot) => Readable::Vec(slot.borrow_unsafe()),
    }
  }

  #[inline]
  fn writable(&mut self) -> Writable<'_, T> {
    match self {
      Slot::EMPTY => unreachable!(),
      Slot::S1(slot) => Writable::Array(slot.as_mut_slice()),
      Slot::S2(slot) => Writable::Array(slot.as_mut_slice()),
      Slot::S4(slot) => Writable::Array(slot.as_mut_slice()),
      Slot::S8(slot) => Writable::Array(slot.as_mut_slice()),
      Slot::S16(slot) => Writable::Array(slot.as_mut_slice()),
      Slot::S32(slot) => Writable::Array(slot.as_mut_slice()),
      Slot::S64(slot) => Writable::Array(slot.as_mut_slice()),
      Slot::S128(slot) => Writable::Array(slot.as_mut_slice()),
      Slot::S256(slot) => Writable::Array(slot.as_mut_slice()),
      Slot::Heap(slot) => Writable::Vec(slot.borrow_mut_unsafe()),
    }
  }
}

/**
 * Dynamically sized vector allocated on the stack.
 */
pub struct Vector<T> {
  slot: Slot<T>,
  len_: usize,
}
impl<T> Vector<T> {
  #[allow(unused)]
  pub fn new() -> Self {
    Self {
      slot: Slot::EMPTY,
      len_: 0,
    }
  }

  pub fn with_capacity(capacity: usize) -> Self {
    Self {
      slot: Slot::from_cap(capacity),
      len_: 0,
    }
  }

  #[inline]
  pub fn push(&mut self, v: T) {
    if self.len_ == self.slot.capacity() {
      self.slot = self.slot.grow();
    }
    match self.slot.writable() {
      Writable::Vec(vec) => {
        vec.push(v);
        self.len_ = vec.len();
      }
      Writable::Array(arr) => {
        arr[self.len_].write(v);
        self.len_ += 1;
      }
    }
  }

  #[inline(always)]
  pub fn len(&self) -> usize {
    self.len_
  }

  #[inline]
  pub fn get(&self, index: usize) -> Option<&T> {
    if self.len_ <= index {
      return None;
    }
    match self.slot.readable() {
      Readable::Vec(vec) => vec.get(index),
      Readable::Array(arr) => arr.get(index).map(|v| unsafe { v.assume_init_ref() }),
    }
  }
  #[inline]
  pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
    if self.len_ <= index {
      return None;
    }
    match self.slot.writable() {
      Writable::Vec(vec) => vec.get_mut(index),
      Writable::Array(arr) => arr.get_mut(index).map(|v| unsafe { v.assume_init_mut() }),
    }
  }
}

impl<T> Index<usize> for Vector<T> {
  type Output = T;

  #[inline]
  fn index(&self, index: usize) -> &Self::Output {
    match self.get(index) {
      Some(v) => v,
      None => panic!(
        "index out of bounds: the len is {} but the index is {index}",
        self.len_
      ),
    }
  }
}
impl<T> IndexMut<usize> for Vector<T> {
  #[inline]
  fn index_mut(&mut self, index: usize) -> &mut Self::Output {
    let len = self.len_;
    match self.get_mut(index) {
      Some(v) => v,
      None => panic!("index out of bounds: the len is {len} but the index is {index}",),
    }
  }
}

impl<T> Drop for Vector<T> {
  fn drop(&mut self) {
    match &mut self.slot {
      Slot::EMPTY => return,
      Slot::S1(slot) => slot
        .iter_mut()
        .take(self.len_)
        .for_each(|v| unsafe { v.assume_init_drop() }),
      Slot::S2(slot) => slot
        .iter_mut()
        .take(self.len_)
        .for_each(|v| unsafe { v.assume_init_drop() }),
      Slot::S4(slot) => slot
        .iter_mut()
        .take(self.len_)
        .for_each(|v| unsafe { v.assume_init_drop() }),
      Slot::S8(slot) => slot
        .iter_mut()
        .take(self.len_)
        .for_each(|v| unsafe { v.assume_init_drop() }),
      Slot::S16(slot) => slot
        .iter_mut()
        .take(self.len_)
        .for_each(|v| unsafe { v.assume_init_drop() }),
      Slot::S32(slot) => slot
        .iter_mut()
        .take(self.len_)
        .for_each(|v| unsafe { v.assume_init_drop() }),
      Slot::S64(slot) => slot
        .iter_mut()
        .take(self.len_)
        .for_each(|v| unsafe { v.assume_init_drop() }),
      Slot::S128(slot) => slot
        .iter_mut()
        .take(self.len_)
        .for_each(|v| unsafe { v.assume_init_drop() }),
      Slot::S256(slot) => slot
        .iter_mut()
        .take(self.len_)
        .for_each(|v| unsafe { v.assume_init_drop() }),
      Slot::Heap(slot) => slot.drop_unsafe(),
    };
  }
}

#[cfg(test)]
#[path = "tests/vec.rs"]
mod tests;
