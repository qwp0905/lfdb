use std::{
  marker::PhantomData,
  mem::ManuallyDrop,
  ptr::{null_mut, NonNull},
  sync::atomic::{AtomicPtr, Ordering},
};

use super::VPtr as VPtrRaw;

type VPtr = VPtrRaw<VTable>;

struct VTable {
  call: unsafe fn(NonNull<()>, NonNull<()>),
  drop_in_place: unsafe fn(NonNull<()>),
}

struct Payload<F, R> {
  function: ManuallyDrop<F>,
  _marker: PhantomData<fn(&R)>,
}
impl<F, R> Payload<F, R> {
  const fn new(f: F) -> Self {
    Self {
      function: ManuallyDrop::new(f),
      _marker: PhantomData,
    }
  }
}
impl<F, R> Payload<F, R>
where
  F: FnOnce(&R),
{
  const VTABLE: VTable = VTable {
    call: Self::call,
    drop_in_place: Self::drop_in_place,
  };
  unsafe fn call(ptr: NonNull<()>, value: NonNull<()>) {
    let this = VPtr::get_mut::<Self>(ptr);
    let function = unsafe { ManuallyDrop::take(&mut this.function) };
    function(value.cast().as_ref());
  }
  unsafe fn drop_in_place(ptr: NonNull<()>) {
    let this = VPtr::get_mut::<Self>(ptr);
    unsafe { ManuallyDrop::drop(&mut this.function) };
  }
}

pub struct Callback<R>(ManuallyDrop<VPtr>, PhantomData<fn(&R)>);
impl<R> Callback<R> {
  pub fn new<F: FnOnce(&R) + Send + 'static>(function: F) -> Self {
    let payload = Payload::<F, R>::new(function);
    let vtable = &Payload::<F, R>::VTABLE;
    let ptr = VPtr::new_box(payload, vtable);
    Self(ManuallyDrop::new(ptr), PhantomData)
  }

  pub fn call(self, value: &R) {
    let mut this = ManuallyDrop::new(self);
    let vptr = unsafe { ManuallyDrop::take(&mut this.0) };
    let vtable = vptr.vtable();
    let ptr = vptr.erased();
    unsafe { (vtable.call)(ptr, NonNull::from_ref(value).cast()) };
  }

  const unsafe fn from_raw(raw: *mut ()) -> Self {
    let vptr = unsafe { VPtr::from_raw(raw) };
    Self(ManuallyDrop::new(vptr), PhantomData)
  }

  fn into_raw(this: Self) -> *mut () {
    let mut this = ManuallyDrop::new(this);
    VPtr::into_raw(unsafe { ManuallyDrop::take(&mut this.0) })
  }
}
impl<R> Drop for Callback<R> {
  fn drop(&mut self) {
    let vptr = unsafe { ManuallyDrop::take(&mut self.0) };
    let vtable = vptr.vtable();
    let ptr = vptr.erased();
    unsafe { (vtable.drop_in_place)(ptr) };
  }
}

static NOTHING: u8 = 0;
const SENTINEL: *mut () = &raw const NOTHING as *mut ();

pub struct CallbackSlot<R>(AtomicPtr<()>, PhantomData<Callback<R>>);
impl<R> CallbackSlot<R> {
  pub const fn new() -> Self {
    Self(AtomicPtr::new(null_mut()), PhantomData)
  }

  pub fn take(&self) -> Option<Callback<R>> {
    let taken = self.0.swap(SENTINEL, Ordering::Acquire);
    if taken.is_null() || taken == SENTINEL {
      return None;
    }
    Some(unsafe { Callback::from_raw(taken) })
  }

  pub fn set(&self, callback: Callback<R>) -> std::result::Result<(), Callback<R>> {
    let raw = Callback::into_raw(callback);
    let mut current = self.0.load(Ordering::Acquire);
    loop {
      if !current.is_null() || current == SENTINEL {
        return Err(unsafe { Callback::from_raw(raw) });
      }
      let Err(err) =
        self
          .0
          .compare_exchange_weak(current, raw, Ordering::Release, Ordering::Acquire)
      else {
        return Ok(());
      };
      current = err;
    }
  }
}
impl<R> Drop for CallbackSlot<R> {
  fn drop(&mut self) {
    let _ = self.take();
  }
}
