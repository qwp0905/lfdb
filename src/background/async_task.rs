use std::{
  cell::UnsafeCell,
  convert::identity,
  marker::PhantomData,
  mem::{ManuallyDrop, MaybeUninit},
  ptr::NonNull,
};

use super::{OneshotBehavior, VPtr as VPtrRaw, WaitDisconnectedError};

type VPtr = VPtrRaw<TaskVTable>;
type WaitResult<T> = std::result::Result<T, WaitDisconnectedError>;

struct TaskVTable {
  fulfill: unsafe fn(NonNull<()>, NonNull<()>),
  wait: unsafe fn(NonNull<()>, NonNull<()>),
  drop_sender: unsafe fn(NonNull<()>),
  drop_receiver: unsafe fn(NonNull<()>),
  drop_in_place: unsafe fn(NonNull<()>),
}

struct TaskPayload<T, R, F> {
  function: UnsafeCell<ManuallyDrop<F>>,
  behavior: OneshotBehavior<R>,
  _marker: PhantomData<fn(T) -> R>,
}
impl<T, R, F> TaskPayload<T, R, F>
where
  F: FnOnce(T) -> R,
{
  const VTABLE: TaskVTable = TaskVTable {
    fulfill: Self::fulfill,
    wait: Self::wait,
    drop_sender: Self::drop_sender,
    drop_receiver: Self::drop_receiver,
    drop_in_place: Self::drop_in_place,
  };

  fn new(function: F) -> Self {
    Self {
      function: UnsafeCell::new(ManuallyDrop::new(function)),
      behavior: OneshotBehavior::new(),
      _marker: PhantomData,
    }
  }

  unsafe fn fulfill(ptr: NonNull<()>, value: NonNull<()>) {
    let this = VPtr::get_ref::<Self>(ptr);
    let func = unsafe { ManuallyDrop::take(&mut (*this.function.get())) };
    let value = unsafe { value.cast::<T>().read() };
    this.behavior.fulfill_and_wake(func(value));
  }
}
impl<T, R, F> TaskPayload<T, R, F> {
  unsafe fn drop_receiver(ptr: NonNull<()>) {
    let this = VPtr::get_ref::<Self>(ptr);
    this.behavior.drop_receiver();
  }
  unsafe fn drop_sender(ptr: NonNull<()>) {
    let this = VPtr::get_ref::<Self>(ptr);
    this.behavior.drop_sender();
  }
  unsafe fn wait(ptr: NonNull<()>, value: NonNull<()>) {
    let this = VPtr::get_ref::<Self>(ptr);
    value.cast::<WaitResult<R>>().write(this.behavior.wait());
  }
  unsafe fn drop_in_place(ptr: NonNull<()>) {
    let this = VPtr::get_ref::<Self>(ptr);
    unsafe { ManuallyDrop::drop(&mut (*this.function.get())) };
  }
}

struct TaskRefInner(VPtr);
impl TaskRefInner {
  unsafe fn fulfill<T>(&self, value: T) {
    let vtable = self.0.vtable();
    let ptr = self.0.erased();
    let value = ManuallyDrop::new(value);
    let value_ptr = NonNull::from_ref(&*value).cast();
    unsafe { (vtable.fulfill)(ptr, value_ptr) };
  }
  unsafe fn drop_in_place(&self) {
    let vtable = self.0.vtable();
    let ptr = self.0.erased();
    unsafe { (vtable.drop_in_place)(ptr) };
  }
}
impl Drop for TaskRefInner {
  fn drop(&mut self) {
    let vtable = self.0.vtable();
    let ptr = self.0.erased();
    unsafe { (vtable.drop_sender)(ptr) };
  }
}

pub struct TaskFulfiller<T> {
  task: ManuallyDrop<TaskRefInner>,
  _marker: PhantomData<T>,
}
impl<T> TaskFulfiller<T> {
  const fn new(ptr: VPtr) -> Self {
    Self {
      task: ManuallyDrop::new(TaskRefInner(ptr)),
      _marker: PhantomData,
    }
  }
  pub fn fulfill(self, value: T) {
    let mut this = ManuallyDrop::new(self);
    let inner = unsafe { ManuallyDrop::take(&mut this.task) };
    unsafe { inner.fulfill(value) };
  }
}
impl<T> Drop for TaskFulfiller<T> {
  fn drop(&mut self) {
    let inner = unsafe { ManuallyDrop::take(&mut self.task) };
    unsafe { inner.drop_in_place() };
  }
}

pub struct AsyncTask<R> {
  task: VPtr,
  _marker: PhantomData<R>,
}
impl<R> AsyncTask<R> {
  const fn new(task: VPtr) -> Self {
    Self {
      task,
      _marker: PhantomData,
    }
  }

  pub fn wait(self) -> WaitResult<R> {
    let vtable = self.task.vtable();
    let ptr = self.task.erased();
    let mut result = MaybeUninit::<WaitResult<R>>::uninit();
    unsafe {
      let result_ptr = NonNull::new_unchecked(result.as_mut_ptr().cast());
      (vtable.wait)(ptr, result_ptr);
      result.assume_init()
    }
  }
}
impl<R> Drop for AsyncTask<R> {
  fn drop(&mut self) {
    let vtable = self.task.vtable();
    let ptr = self.task.erased();
    unsafe { (vtable.drop_receiver)(ptr) };
  }
}

pub fn create_task<T: Send + 'static>() -> (TaskFulfiller<T>, AsyncTask<T>) {
  create_task_with(identity)
}

pub fn create_task_with<
  T: Send + 'static,
  R: Send + 'static,
  F: FnOnce(T) -> R + Send + 'static,
>(
  callback: F,
) -> (TaskFulfiller<T>, AsyncTask<R>) {
  let task = TaskPayload::new(callback);
  let (p1, p2) = VPtr::new_pair(task, &TaskPayload::<T, R, F>::VTABLE);
  (TaskFulfiller::new(p1), AsyncTask::new(p2))
}

#[cfg(all(test, not(loom)))]
#[path = "tests/async_task.rs"]
mod tests;
