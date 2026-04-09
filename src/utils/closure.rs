use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  sync::Arc,
};

pub trait SafeCallable<T, R> {
  type Error;
  fn safe_call(&self, v: T) -> std::result::Result<R, Self::Error>;
}
impl<T, R, F> SafeCallable<T, R> for F
where
  T: UnwindSafe,
  F: Fn(T) -> R + RefUnwindSafe,
{
  type Error = Arc<dyn std::any::Any + Send>;

  #[inline(always)]
  fn safe_call(&self, v: T) -> std::result::Result<R, Self::Error> {
    std::panic::catch_unwind(|| self(v)).map_err(Arc::from)
  }
}

// pub trait SafeCallableMut<T, R> {
//   type Error;
//   fn safe_call_mut(&mut self, v: T) -> std::result::Result<R, Self::Error>;
// }
// impl<T, R, F> SafeCallableMut<T, R> for F
// where
//   T: UnwindSafe,
//   F: FnMut(T) -> R + RefUnwindSafe,
// {
//   type Error = Arc<dyn std::any::Any + Send>;

//   #[inline(always)]
//   fn safe_call_mut(&mut self, v: T) -> std::result::Result<R, Self::Error> {
//     let ptr = self as *mut Self;
//     std::panic::catch_unwind(|| unsafe { &mut *ptr }(v)).map_err(Arc::from)
//   }
// }
