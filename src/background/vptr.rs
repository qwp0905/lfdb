use std::{mem::forget, ptr::NonNull};

use super::Pair;

unsafe fn drop_pair<T, VTable>(ptr: NonNull<Header<VTable>>) {
  let _ = Pair::from_raw(ptr.as_ptr() as *mut VObject<T, VTable>);
}
unsafe fn drop_box<T, VTable>(ptr: NonNull<Header<VTable>>) {
  let _ = Box::from_raw(ptr.as_ptr() as *mut VObject<T, VTable>);
}
struct Header<VTable: 'static> {
  vtable: &'static VTable,
  drop: Option<unsafe fn(NonNull<Self>)>,
}
impl<VTable> Header<VTable> {
  const fn new_pair<T>(vtable: &'static VTable) -> Self {
    Self {
      vtable,
      drop: Some(drop_pair::<T, VTable>),
    }
  }
  const fn new_box<T>(vtable: &'static VTable) -> Self {
    Self {
      vtable,
      drop: Some(drop_box::<T, VTable>),
    }
  }
}

#[repr(C)]
struct VObject<T, VTable: 'static> {
  header: Header<VTable>,
  payload: T,
}
impl<T, VTable> VObject<T, VTable> {
  const fn construct(header: Header<VTable>, payload: T) -> Self {
    Self { header, payload }
  }
}

pub struct VPtr<VTable: 'static>(NonNull<Header<VTable>>);
impl<VTable> VPtr<VTable> {
  pub fn new_pair<T: Send>(payload: T, vtable: &'static VTable) -> (Self, Self) {
    let inner = VObject::construct(Header::new_pair::<T>(vtable), payload);
    let (p1, p2) = Pair::new(inner);

    let p1 = unsafe { NonNull::new_unchecked(Pair::into_raw(p1)) };
    let p2 = unsafe { NonNull::new_unchecked(Pair::into_raw(p2)) };
    (Self(p1.cast()), Self(p2.cast()))
  }
  pub fn new_box<T: Send>(payload: T, vtable: &'static VTable) -> Self {
    let inner = VObject::construct(Header::new_box::<T>(vtable), payload);
    let p = NonNull::from_mut(Box::leak(Box::new(inner)));
    Self(p.cast())
  }

  pub const fn into_raw(this: Self) -> *mut () {
    let ptr = this.0.as_ptr();
    forget(this);
    ptr.cast()
  }
  pub const unsafe fn from_raw(raw: *mut ()) -> Self {
    let ptr = raw as *mut Header<VTable>;
    Self(unsafe { NonNull::new_unchecked(ptr) })
  }

  pub const fn vtable(&self) -> &'static VTable {
    unsafe { self.0.as_ref() }.vtable
  }
  pub const fn erased(&self) -> NonNull<()> {
    self.0.cast()
  }

  pub const unsafe fn get_ref<'a, T>(ptr: NonNull<()>) -> &'a T {
    let raw = ptr.cast::<VObject<T, VTable>>();
    &raw.as_ref().payload
  }
}
impl<VTable> Drop for VPtr<VTable> {
  fn drop(&mut self) {
    let ptr = self.0;
    unsafe {
      if let Some(drop_fn) = self.0.as_ref().drop {
        drop_fn(ptr);
      };
    };
  }
}

unsafe impl<VTable: Sync> Send for VPtr<VTable> {}
unsafe impl<VTable: Sync> Sync for VPtr<VTable> {}
