use std::{
  cell::UnsafeCell,
  panic::{RefUnwindSafe, UnwindSafe},
};

pub struct UnsafeOption<T>(UnsafeCell<Option<T>>);
impl<T> UnsafeOption<T> {
  const fn new(inner: Option<T>) -> Self {
    Self(UnsafeCell::new(inner))
  }
  pub const fn none() -> Self {
    Self::new(None)
  }
  pub const fn some(inner: T) -> Self {
    Self::new(Some(inner))
  }

  pub const fn get_mut(&self) -> &mut Option<T> {
    unsafe { &mut *self.0.get() }
  }
  pub const fn get(&self) -> &Option<T> {
    unsafe { &*self.0.get() }
  }
}
impl<T> Default for UnsafeOption<T> {
  fn default() -> Self {
    Self(Default::default())
  }
}
unsafe impl<T: Send> Send for UnsafeOption<T> {}
unsafe impl<T: Sync> Sync for UnsafeOption<T> {}
impl<T: RefUnwindSafe> RefUnwindSafe for UnsafeOption<T> {}
impl<T: UnwindSafe> UnwindSafe for UnsafeOption<T> {}
