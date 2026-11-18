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
  context: unsafe fn(NonNull<()>) -> NonNull<()>,
  drop_sender: unsafe fn(NonNull<()>),
  drop_receiver: unsafe fn(NonNull<()>),
  drop_in_place: unsafe fn(NonNull<()>),
}

struct TaskPayload<T, R, F, C> {
  function: UnsafeCell<ManuallyDrop<F>>,
  behavior: OneshotBehavior<R>,
  context: C,
  _marker: PhantomData<fn(T) -> R>,
}
impl<T, R, F, C> TaskPayload<T, R, F, C>
where
  F: FnOnce(T) -> R,
{
  const VTABLE: TaskVTable = TaskVTable {
    fulfill: Self::fulfill,
    wait: Self::wait,
    drop_sender: Self::drop_sender,
    drop_receiver: Self::drop_receiver,
    drop_in_place: Self::drop_in_place,
    context: Self::context,
  };

  fn new(function: F, context: C) -> Self {
    Self {
      function: UnsafeCell::new(ManuallyDrop::new(function)),
      behavior: OneshotBehavior::new(),
      context,
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
impl<T, R, F, C> TaskPayload<T, R, F, C> {
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
  unsafe fn context(ptr: NonNull<()>) -> NonNull<()> {
    let this = VPtr::get_ref::<Self>(ptr);
    NonNull::from_ref(&this.context).cast()
  }
}

struct TaskRefInner(ManuallyDrop<VPtr>);
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
  unsafe fn context(&self) -> NonNull<()> {
    let vtable = self.0.vtable();
    let ptr = self.0.erased();
    unsafe { (vtable.context)(ptr) }
  }
}
impl Drop for TaskRefInner {
  fn drop(&mut self) {
    let vtable = self.0.vtable();
    let ptr = self.0.erased();
    unsafe { (vtable.drop_sender)(ptr) };
    unsafe { ManuallyDrop::drop(&mut self.0) };
  }
}

pub struct TaskFulfiller<T, C = ()> {
  task: ManuallyDrop<TaskRefInner>,
  _marker: PhantomData<(fn(T), C)>,
}
impl<T, C> TaskFulfiller<T, C> {
  const fn new(ptr: VPtr) -> Self {
    Self {
      task: ManuallyDrop::new(TaskRefInner(ManuallyDrop::new(ptr))),
      _marker: PhantomData,
    }
  }
  pub fn fulfill(self, value: T) {
    let mut this = ManuallyDrop::new(self);
    let inner = unsafe { ManuallyDrop::take(&mut this.task) };
    unsafe { inner.fulfill(value) };
  }
  pub fn context(&self) -> &'_ C {
    let ptr = unsafe { self.task.context() };
    unsafe { ptr.cast().as_ref() }
  }

  #[cfg(target_os = "linux")]
  pub fn into_raw(this: Self) -> *mut () {
    let mut this = ManuallyDrop::new(this);
    VPtr::into_raw(unsafe { ManuallyDrop::take(&mut this.task.0) })
  }
  #[cfg(target_os = "linux")]
  pub unsafe fn from_raw(raw: *mut ()) -> Self {
    Self::new(unsafe { VPtr::from_raw(raw) })
  }
}
impl<T, C> Drop for TaskFulfiller<T, C> {
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
  create_tagged_task(())
}

pub fn create_task_with<
  T: Send + 'static,
  R: Send + 'static,
  F: FnOnce(T) -> R + Send + 'static,
>(
  callback: F,
) -> (TaskFulfiller<T>, AsyncTask<R>) {
  create_tagged_task_with((), callback)
}

pub fn create_tagged_task<T: Send + 'static, C: Send + 'static>(
  context: C,
) -> (TaskFulfiller<T, C>, AsyncTask<T>) {
  create_tagged_task_with(context, identity)
}
pub fn create_tagged_task_with<
  T: Send + 'static,
  R: Send + 'static,
  F: FnOnce(T) -> R + Send + 'static,
  C: Send + 'static,
>(
  context: C,
  callback: F,
) -> (TaskFulfiller<T, C>, AsyncTask<R>) {
  let task = TaskPayload::new(callback, context);
  let (p1, p2) = VPtr::new_pair(task, &TaskPayload::<T, R, F, C>::VTABLE);
  (TaskFulfiller::new(p1), AsyncTask::new(p2))
}

#[cfg(all(test, not(loom)))]
#[path = "tests/async_task.rs"]
mod tests;
