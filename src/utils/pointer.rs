use std::sync::Arc;

pub trait ToArc {
  fn to_arc(self) -> Arc<Self>;
}
impl<T> ToArc for T {
  #[inline(always)]
  fn to_arc(self) -> Arc<Self> {
    Arc::new(self)
  }
}

pub trait ToBox {
  fn to_box(self) -> Box<Self>;
}
impl<T> ToBox for T {
  #[inline(always)]
  fn to_box(self) -> Box<Self> {
    Box::new(self)
  }
}

pub trait ToRawPointer {
  fn to_raw_ptr(self) -> *mut Self;
}
impl<T> ToRawPointer for T {
  #[inline(always)]
  fn to_raw_ptr(self) -> *mut Self {
    Box::into_raw(Box::new(self))
  }
}

pub trait UnsafeBorrow<'a, T: 'a> {
  fn borrow_unsafe(self) -> &'a T;
}
impl<'a, T: 'a> UnsafeBorrow<'a, T> for *const T {
  #[inline(always)]
  fn borrow_unsafe(self) -> &'a T {
    unsafe { &*self }
  }
}
impl<'a, T: 'a> UnsafeBorrow<'a, T> for *mut T {
  #[inline(always)]
  fn borrow_unsafe(self) -> &'a T {
    unsafe { &*self }
  }
}

pub trait UnsafeBorrowMut<'a, T: 'a> {
  fn borrow_mut_unsafe(self) -> &'a mut T;
}
impl<'a, T: 'a> UnsafeBorrowMut<'a, T> for *mut T {
  #[inline(always)]
  fn borrow_mut_unsafe(self) -> &'a mut T {
    unsafe { &mut *self }
  }
}

pub trait UnsafeTake<T> {
  fn take_unsafe(self) -> T;
}
impl<T> UnsafeTake<T> for *const T {
  #[inline(always)]
  fn take_unsafe(self) -> T {
    unsafe { *Box::from_raw(self as *mut T) }
  }
}
impl<T> UnsafeTake<T> for *mut T {
  #[inline(always)]
  fn take_unsafe(self) -> T {
    unsafe { *Box::from_raw(self) }
  }
}

pub trait UnsafeDrop<T> {
  fn drop_unsafe(self);
}
impl<T> UnsafeDrop<T> for *const T {
  #[inline(always)]
  fn drop_unsafe(self) {
    let _ = self.take_unsafe();
  }
}
impl<T> UnsafeDrop<T> for *mut T {
  #[inline(always)]
  fn drop_unsafe(self) {
    let _ = self.take_unsafe();
  }
}
