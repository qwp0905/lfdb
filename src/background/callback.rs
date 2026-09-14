use std::{
  cell::UnsafeCell,
  marker::PhantomData,
  ptr::{null_mut, NonNull},
  sync::atomic::{AtomicPtr, Ordering},
};

use super::VPtr as VPtrRaw;

type VPtr = VPtrRaw<VTable>;

struct VTable {
  call: unsafe fn(NonNull<()>, NonNull<()>),
}

struct Payload<F, R> {
  function: UnsafeCell<Option<F>>,
  _marker: PhantomData<fn(&R)>,
}
impl<F, R> Payload<F, R> {
  const fn new(f: F) -> Self {
    Self {
      function: UnsafeCell::new(Some(f)),
      _marker: PhantomData,
    }
  }
}
impl<F, R> Payload<F, R>
where
  F: FnOnce(&R),
{
  const VTABLE: VTable = VTable { call: Self::call };
  unsafe fn call(ptr: NonNull<()>, value: NonNull<()>) {
    let this = VPtr::get_ref::<Self>(ptr);
    let function = (*this.function.get()).take().unwrap();
    function(value.cast().as_ref());
  }
}

pub struct Callback(VPtr);
impl Callback {
  pub fn new<F: FnOnce(&R) + Send, R>(function: F) -> Self {
    let payload = Payload::<F, R>::new(function);
    let vtable = &Payload::<F, R>::VTABLE;
    let ptr = VPtr::new_box(payload, vtable);
    Self(ptr)
  }

  pub unsafe fn call<R>(&self, value: &R) {
    let vtable = self.0.vtable();
    let ptr = self.0.erased();
    unsafe { (vtable.call)(ptr, NonNull::from_ref(value).cast()) };
  }

  unsafe fn into_inner<F, R>(self) -> F {
    let payload = self.0.into_boxed_inner::<Payload<F, R>>();
    payload.function.into_inner().unwrap()
  }

  const unsafe fn from_raw(raw: *mut ()) -> Self {
    Self(unsafe { VPtr::from_raw(raw) })
  }

  fn into_raw(this: Self) -> *mut () {
    VPtr::into_raw(this.0)
  }
}

static NOTHING: u8 = 0;
const SENTINEL: *mut () = &raw const NOTHING as *mut ();

pub struct CallbackSlot(AtomicPtr<()>);
impl CallbackSlot {
  pub const fn new() -> Self {
    Self(AtomicPtr::new(null_mut()))
  }

  pub fn take(&self) -> Option<Callback> {
    let taken = self.0.swap(SENTINEL, Ordering::Acquire);
    if taken.is_null() || taken == SENTINEL {
      return None;
    }
    Some(unsafe { Callback::from_raw(taken) })
  }

  pub fn set<F: FnOnce(&R) + Send + 'static, R>(
    &self,
    callback: F,
  ) -> std::result::Result<(), F> {
    let raw = Callback::into_raw(Callback::new(callback));
    let mut current = self.0.load(Ordering::Acquire);
    loop {
      if !current.is_null() || current == SENTINEL {
        return Err(unsafe { Callback::from_raw(raw).into_inner::<F, R>() });
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
impl Drop for CallbackSlot {
  fn drop(&mut self) {
    let _ = self.take();
  }
}
