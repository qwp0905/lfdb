use std::ptr::NonNull;

use super::Pair;

struct Header<VTable: 'static> {
  vtable: &'static VTable,
  drop: unsafe fn(NonNull<Self>),
}
impl<VTable> Header<VTable> {
  const fn new_pair<T>(vtable: &'static VTable) -> Self {
    Self {
      vtable,
      drop: Self::drop_pair::<T>,
    }
  }
  unsafe fn drop_pair<T>(ptr: NonNull<Header<VTable>>) {
    let _ = Pair::from_raw(ptr.as_ptr() as *mut VObject<T, VTable>);
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
    let drop_fn = unsafe { self.0.as_ref().drop };
    unsafe { drop_fn(ptr) };
  }
}

unsafe impl<VTable: Sync> Send for VPtr<VTable> {}
unsafe impl<VTable: Sync> Sync for VPtr<VTable> {}
