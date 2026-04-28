use std::ptr::NonNull;

use super::Bucket;

pub struct LRUList<K, V> {
  head: Option<NonNull<Bucket<K, V>>>,
  tail: Option<NonNull<Bucket<K, V>>>,
  len: usize,
}
impl<K, V> LRUList<K, V> {
  pub const fn new() -> Self {
    Self {
      head: None,
      tail: None,
      len: 0,
    }
  }

  pub const fn push_head(&mut self, bucket: &mut NonNull<Bucket<K, V>>) {
    match &self.head {
      Some(mut head) => unsafe {
        head.as_mut().set_prev(Some(*bucket));
        bucket.as_mut().set_next(Some(head));
      },
      None => {
        self.tail = Some(*bucket);
      }
    }
    self.head = Some(*bucket);

    self.len += 1;
  }

  pub const fn remove(&mut self, bucket: &mut NonNull<Bucket<K, V>>) {
    if self.len == 0 {
      return;
    }

    let bucket = unsafe { bucket.as_mut() };
    let n = bucket.set_next(None);
    let p = bucket.set_prev(None);

    if let Some(mut next) = &n {
      unsafe { next.as_mut() }.set_prev(p);
    } else {
      self.tail = p;
    }

    if let Some(mut prev) = &p {
      unsafe { prev.as_mut() }.set_next(n);
    } else {
      self.head = n;
    }

    self.len -= 1;
  }

  pub const fn move_to_head(&mut self, bucket: &mut NonNull<Bucket<K, V>>) {
    self.remove(bucket);
    self.push_head(bucket);
  }

  pub const fn pop_tail(&mut self) -> Option<NonNull<Bucket<K, V>>> {
    if let Some(mut tail) = self.tail {
      self.remove(&mut tail);
      return Some(tail);
    }
    None
  }

  pub const fn len(&self) -> usize {
    self.len
  }
}
