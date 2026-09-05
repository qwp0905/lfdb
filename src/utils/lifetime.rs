use std::mem::transmute;

pub const unsafe fn create_static_ref<T: ?Sized>(v: &T) -> &'static T {
  unsafe { transmute::<&T, &'static T>(v) }
}
