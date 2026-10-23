use std::{
  mem::{forget, offset_of},
  ops::Deref,
  ptr::NonNull,
  sync::atomic::Ordering,
};

use super::loom::{fence, AtomicBool};

#[repr(C)]
struct Inner<T: ?Sized> {
  dropped: AtomicBool,
  value: T,
}
impl<T> Inner<T> {
  fn new(value: T) -> Self {
    Self {
      dropped: AtomicBool::new(false),
      value,
    }
  }
}

/**
 * `Arc` fixed to exactly two handles.
 */
pub struct TwBox<T: ?Sized>(NonNull<Inner<T>>);
impl<T> TwBox<T> {
  pub fn new(value: T) -> (Self, Self) {
    let ptr = NonNull::from_mut(Box::leak(Box::new(Inner::new(value))));
    (Self(ptr), Self(ptr))
  }

  pub const fn into_raw(this: Self) -> *mut T {
    let ptr = this.as_ptr();
    forget(this);
    ptr
  }

  pub const unsafe fn from_raw(ptr: *mut T) -> Self {
    let offset = offset_of!(Inner<T>, value);
    let ptr = (ptr as *mut u8).sub(offset) as *mut Inner<T>;
    Self(NonNull::new_unchecked(ptr))
  }

  const fn as_ptr(&self) -> *mut T {
    unsafe { &raw mut (*self.0.as_ptr()).value }
  }
}
impl<T: ?Sized> Drop for TwBox<T> {
  fn drop(&mut self) {
    if !unsafe { self.0.as_ref() }
      .dropped
      .fetch_or(true, Ordering::Release)
    {
      return;
    }
    fence(Ordering::Acquire);
    let _ = unsafe { Box::from_raw(self.0.as_ptr()) };
  }
}
impl<T: ?Sized> Deref for TwBox<T> {
  type Target = T;

  fn deref(&self) -> &Self::Target {
    unsafe { &self.0.as_ref().value }
  }
}
unsafe impl<T: Send + Sync + ?Sized> Send for TwBox<T> {}
unsafe impl<T: Send + Sync + ?Sized> Sync for TwBox<T> {}

#[cfg(all(test, loom))]
#[path = "tests/twbox_loom.rs"]
mod tests;
