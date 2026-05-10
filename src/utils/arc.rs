#![allow(unsafe_code)]

use std::{
  ops::Deref,
  sync::atomic::{fence, AtomicUsize, Ordering},
};

#[repr(C)]
struct Inner<T: ?Sized> {
  count: AtomicUsize,
  data: T,
}

pub struct SArc<T: ?Sized> {
  pointer: *mut Inner<T>,
}
impl<T> SArc<T> {
  pub fn new(data: T) -> Self {
    let inner = Inner {
      count: AtomicUsize::new(1),
      data,
    };
    Self {
      pointer: Box::into_raw(Box::new(inner)),
    }
  }
}
impl<T: ?Sized> Clone for SArc<T> {
  fn clone(&self) -> Self {
    if unsafe { &*self.pointer }
      .count
      .fetch_add(1, Ordering::Relaxed)
      == usize::MAX
    {
      std::process::abort();
    };
    Self {
      pointer: self.pointer,
    }
  }
}

impl<T: ?Sized> Drop for SArc<T> {
  fn drop(&mut self) {
    if unsafe { &*self.pointer }
      .count
      .fetch_sub(1, Ordering::Acquire)
      > 1
    {
      return;
    }

    fence(Ordering::Acquire);
    let _ = unsafe { Box::from_raw(self.pointer) };
  }
}

unsafe impl<T: Send + Sync + ?Sized> Send for SArc<T> {}
unsafe impl<T: Send + Sync + ?Sized> Sync for SArc<T> {}

impl<T: ?Sized> Deref for SArc<T> {
  type Target = T;

  fn deref(&self) -> &T {
    unsafe { &(*self.pointer).data }
  }
}

// impl<T: ?Sized> std::borrow::Borrow<T> for SArc<T> {
//   fn borrow(&self) -> &T {
//     &**self
//   }
// }

impl<T: ?Sized> AsRef<T> for SArc<T> {
  fn as_ref(&self) -> &T {
    &**self
  }
}

#[cfg(test)]
#[path = "tests/arc.rs"]
mod tests;
