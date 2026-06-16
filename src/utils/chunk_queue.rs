use std::{mem::MaybeUninit, ptr::null_mut};

const CAP: usize = 31;

struct Block<T> {
  next: *mut Block<T>,
  data: [MaybeUninit<T>; CAP],
  head: usize,
  tail: usize,
}
impl<T> Block<T> {
  const fn push(&mut self, value: T) {
    self.data[self.tail].write(value);
    self.tail += 1;
  }
  const fn new(value: T) -> Self {
    let mut data = [const { MaybeUninit::uninit() }; CAP];
    data[0].write(value);
    Self {
      next: null_mut(),
      data,
      head: 0,
      tail: 1,
    }
  }
  const fn pop(&mut self) -> T {
    let value = unsafe { self.data[self.head].assume_init_read() };
    self.head += 1;
    value
  }
  const fn is_full(&self) -> bool {
    self.tail == CAP
  }
  const fn is_empty(&self) -> bool {
    self.head == self.tail
  }
}
impl<T> Drop for Block<T> {
  fn drop(&mut self) {
    for i in self.head..self.tail {
      unsafe { self.data[i].assume_init_drop() };
    }
  }
}

pub struct ChunkQueue<T> {
  head: *mut Block<T>,
  tail: *mut Block<T>,
  len: usize,
}
impl<T> ChunkQueue<T> {
  pub const fn new() -> Self {
    Self {
      head: null_mut(),
      tail: null_mut(),
      len: 0,
    }
  }
  #[cfg(test)]
  pub const fn len(&self) -> usize {
    self.len
  }
  #[cfg(test)]
  pub const fn is_empty(&self) -> bool {
    self.len == 0
  }
  pub fn push(&mut self, value: T) {
    self.len += 1;
    if self.head.is_null() {
      self.tail = Box::into_raw(Box::new(Block::new(value)));
      self.head = self.tail;
      return;
    }

    let tail = unsafe { &mut *self.tail };
    if !tail.is_full() {
      return tail.push(value);
    };

    tail.next = Box::into_raw(Box::new(Block::new(value)));
    self.tail = tail.next;
  }
  pub fn pop(&mut self) -> Option<T> {
    while !self.head.is_null() {
      let head = unsafe { &mut *self.head };
      if !head.is_empty() {
        self.len -= 1;
        return Some(head.pop());
      }
      self.head = unsafe { Box::from_raw(self.head) }.next;
    }
    None
  }
}
impl<T> Drop for ChunkQueue<T> {
  fn drop(&mut self) {
    let mut ptr = self.head;
    while !ptr.is_null() {
      ptr = unsafe { Box::from_raw(ptr) }.next;
    }
  }
}
unsafe impl<T: Send> Send for ChunkQueue<T> {}
unsafe impl<T: Sync> Sync for ChunkQueue<T> {}

#[cfg(test)]
#[path = "tests/chunk_queue.rs"]
mod tests;
