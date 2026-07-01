/**
 * Small convenience traits for common pointer.
 *
 * This module is intentionally lightweight. It only shortens repetitive code
 * such as `Arc::new(value)`, `Box::new(value)`.
 */
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
