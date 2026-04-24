use std::ptr::NonNull;

use crate::utils::UnsafeBorrowMut;

use super::Bucket;

pub struct LRUList<K, V> {
  head: Option<NonNull<Bucket<K, V>>>,
  tail: Option<NonNull<Bucket<K, V>>>,
  len_: usize,
}
impl<K, V> LRUList<K, V> {
  pub fn new() -> Self {
    Self {
      head: None,
      tail: None,
      len_: 0,
    }
  }

  // pub fn clear(&mut self) {
  //   self.head = None;
  //   self.tail = None;
  //   self.len_ = 0;
  // }

  pub fn push_head(&mut self, bucket: &mut NonNull<Bucket<K, V>>) {
    self.len_ += 1;
    match &self.head {
      Some(head) => {
        head.borrow_mut_unsafe().set_prev(Some(*bucket));
        bucket.borrow_mut_unsafe().set_next(Some(*head));
      }
      None => {
        self.tail = Some(*bucket);
      }
    }
    self.head = Some(*bucket)
  }

  pub fn remove(&mut self, bucket: &mut NonNull<Bucket<K, V>>) {
    if self.len_ == 0 {
      return;
    }

    let bucket = bucket.borrow_mut_unsafe();
    let n = bucket.set_next(None);
    let p = bucket.set_prev(None);

    if let Some(next) = &n {
      next.borrow_mut_unsafe().set_prev(p.clone());
    } else {
      self.tail = p;
    }

    if let Some(prev) = &p {
      prev.borrow_mut_unsafe().set_next(n);
    } else {
      self.head = n;
    }

    self.len_ -= 1;
  }

  pub fn move_to_head(&mut self, bucket: &mut NonNull<Bucket<K, V>>) {
    self.remove(bucket);
    self.push_head(bucket);
  }

  pub fn pop_tail(&mut self) -> Option<NonNull<Bucket<K, V>>> {
    let mut tail = self.tail?;
    self.remove(&mut tail);
    Some(tail)
  }

  pub fn len(&self) -> usize {
    self.len_
  }
}
