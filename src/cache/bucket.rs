use std::{mem::replace, ptr::NonNull};

pub struct Bucket<K, V> {
  key: K,
  value: V,
  prev: Option<NonNull<Bucket<K, V>>>,
  next: Option<NonNull<Bucket<K, V>>>,
}
impl<K, V> Bucket<K, V> {
  const fn new(key: K, value: V) -> Self {
    Self {
      key,
      value,
      prev: None,
      next: None,
    }
  }

  pub fn new_ptr(key: K, value: V) -> NonNull<Self> {
    NonNull::from(Box::leak(Box::new(Self::new(key, value))))
  }

  pub const fn set_prev(
    &mut self,
    prev: Option<NonNull<Bucket<K, V>>>,
  ) -> Option<NonNull<Bucket<K, V>>> {
    replace(&mut self.prev, prev)
  }

  pub const fn set_next(
    &mut self,
    next: Option<NonNull<Bucket<K, V>>>,
  ) -> Option<NonNull<Bucket<K, V>>> {
    replace(&mut self.next, next)
  }

  pub const fn get_value(&self) -> &V {
    &self.value
  }

  pub const fn set_value(&mut self, value: V) -> V {
    replace(&mut self.value, value)
  }

  pub const fn get_key(&self) -> &K {
    &self.key
  }

  pub fn take(self) -> (K, V) {
    (self.key, self.value)
  }
}
