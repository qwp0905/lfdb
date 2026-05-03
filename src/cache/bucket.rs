use std::{
  mem::replace,
  ptr::NonNull,
  time::{Duration, Instant},
};
const PROMOTE_THRESHOLD: Duration = Duration::from_secs(1);

pub struct Bucket<K, V> {
  key: K,
  value: V,
  prev: Option<NonNull<Bucket<K, V>>>,
  next: Option<NonNull<Bucket<K, V>>>,
  last_promoted: Instant,
}
impl<K, V> Bucket<K, V> {
  fn new(key: K, value: V) -> Self {
    Self {
      key,
      value,
      prev: None,
      next: None,
      last_promoted: Instant::now(),
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

  pub fn promote(&mut self) -> bool {
    if self.last_promoted.elapsed() < PROMOTE_THRESHOLD {
      return false;
    }

    self.last_promoted = Instant::now();
    true
  }
}
