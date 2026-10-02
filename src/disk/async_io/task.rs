use std::cell::UnsafeCell;
use std::fs::File;
use std::io::{IoSlice, Result};
use std::mem::{forget, MaybeUninit};
use std::ptr::NonNull;
use std::sync::Arc;

use crate::background::{Callback, OneshotBehavior, VPtr};

pub enum TaskType {
  Pwrite {
    offset: u64,
    buf: &'static [u8],
  },
  Pwritev {
    offset: u64,
    bufs: &'static [IoSlice<'static>],
  },
  Fsync,
  Fdatasync,
  Fallocate {
    offset: u64,
    len: u64,
  },
}

pub struct FullTask {
  pub toward: Arc<File>,
  pub task_type: TaskType,
  pub done: AsyncTask<usize>,
}

type AddCallback<T> = unsafe fn(
  NonNull<()>,
  Callback<Result<T>>,
) -> std::result::Result<(), Callback<Result<T>>>;

struct VTable<T> {
  fulfill: unsafe fn(NonNull<()>, Result<T>),
  wait: unsafe fn(NonNull<()>, NonNull<Result<T>>),
  add_callback: AddCallback<T>,
  drop_sender: unsafe fn(NonNull<()>),
  drop_receiver: unsafe fn(NonNull<()>),
}
struct Payload<T, F> {
  callback: UnsafeCell<Option<F>>,
  behavior: OneshotBehavior<Result<T>>,
}
impl<T: 'static, F: FnOnce(&Result<T>)> Payload<T, F> {
  const VTABLE: VTable<T> = VTable {
    fulfill: Self::fulfill,
    wait: Self::wait,
    add_callback: Self::add_callback,
    drop_receiver: Self::drop_receiver,
    drop_sender: Self::drop_sender,
  };
  fn new(callback: Option<F>) -> Self {
    Self {
      callback: UnsafeCell::new(callback),
      behavior: OneshotBehavior::new(),
    }
  }
  unsafe fn fulfill(ptr: NonNull<()>, result: Result<T>) {
    let this = VPtr::<VTable<T>>::get_ref::<Self>(ptr);
    if let Some(callback) = (*this.callback.get()).take() {
      callback(&result);
    }
    this.behavior.fulfill_and_wake(result);
  }
  unsafe fn wait(ptr: NonNull<()>, result: NonNull<Result<T>>) {
    let this = VPtr::<VTable<T>>::get_ref::<Self>(ptr);
    result.write(this.behavior.wait().unwrap());
  }
  unsafe fn add_callback(
    ptr: NonNull<()>,
    callback: Callback<Result<T>>,
  ) -> std::result::Result<(), Callback<Result<T>>> {
    let this = VPtr::<VTable<T>>::get_ref::<Self>(ptr);
    this.behavior.add_callback(callback)
  }
  unsafe fn drop_sender(ptr: NonNull<()>) {
    let this = VPtr::<VTable<T>>::get_ref::<Self>(ptr);
    this.behavior.drop_sender();
  }
  unsafe fn drop_receiver(ptr: NonNull<()>) {
    let this = VPtr::<VTable<T>>::get_ref::<Self>(ptr);
    this.behavior.drop_receiver();
  }
}
pub struct AsyncTask<T: 'static>(VPtr<VTable<T>>);
impl<T> AsyncTask<T> {
  pub fn new<F>(callback: Option<F>) -> (Self, PendingAsync<T>)
  where
    T: Send,
    F: FnOnce(&Result<T>) + Send + 'static,
  {
    let payload = Payload::new(callback);
    let vtable = &Payload::<T, F>::VTABLE;

    let (p1, p2) = VPtr::new_pair(payload, vtable);
    (Self(p1), PendingAsync(p2))
  }

  pub const fn into_raw(this: Self) -> *mut () {
    let ptr = this.0.erased();
    forget(this);
    ptr.as_ptr()
  }
  pub const unsafe fn from_raw(raw: *mut ()) -> Self {
    let ptr = unsafe { VPtr::from_raw(raw) };
    Self(ptr)
  }

  pub fn fulfill(self, result: Result<T>) {
    let vtable = self.0.vtable();
    let ptr = self.0.erased();
    unsafe { (vtable.fulfill)(ptr, result) };
  }
}
impl<T> Drop for AsyncTask<T> {
  fn drop(&mut self) {
    let vtable = self.0.vtable();
    let ptr = self.0.erased();
    unsafe { (vtable.drop_sender)(ptr) };
  }
}
pub struct PendingAsync<T: 'static>(VPtr<VTable<T>>);
impl<T> PendingAsync<T> {
  pub fn wait(self) -> Result<T> {
    let vtable = self.0.vtable();
    let ptr = self.0.erased();
    let mut result = MaybeUninit::uninit();
    unsafe { (vtable.wait)(ptr, NonNull::new_unchecked(result.as_mut_ptr())) };
    unsafe { result.assume_init() }
  }

  pub fn must_call(self, callback: Callback<Result<T>>) {
    let vtable = self.0.vtable();
    let ptr = self.0.erased();
    if let Err(err) = unsafe { (vtable.add_callback)(ptr, callback) } {
      err.call(&self.wait())
    }
  }
}
impl<T> Drop for PendingAsync<T> {
  fn drop(&mut self) {
    let vtable = self.0.vtable();
    let ptr = self.0.erased();
    unsafe { (vtable.drop_receiver)(ptr) };
  }
}
