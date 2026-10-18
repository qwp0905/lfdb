use std::{
  cell::UnsafeCell,
  marker::PhantomData,
  mem::{ManuallyDrop, MaybeUninit},
  ptr::NonNull,
};

use super::{OneshotBehavior, VPtr as VPtrRaw, WaitDisconnectedError};

type WaitResult<T> = std::result::Result<T, WaitDisconnectedError>;

type VPtr = VPtrRaw<TaskVTable>;

struct TaskVTable {
  run: unsafe fn(NonNull<()>),
  wait: unsafe fn(NonNull<()>, NonNull<()>),
  drop_sender: unsafe fn(NonNull<()>),
  drop_receiver: unsafe fn(NonNull<()>),
  drop_in_place: unsafe fn(NonNull<()>),
}

struct TaskPayload<F, R> {
  function: UnsafeCell<ManuallyDrop<F>>,
  behavior: OneshotBehavior<R>,
}
impl<F, R> TaskPayload<F, R>
where
  F: FnOnce() -> R,
{
  const VTABLE: TaskVTable = TaskVTable {
    run: Self::run,
    wait: Self::wait,
    drop_sender: Self::drop_sender,
    drop_receiver: Self::drop_receiver,
    drop_in_place: Self::drop_in_place,
  };

  const fn new(function: F) -> Self {
    Self {
      function: UnsafeCell::new(ManuallyDrop::new(function)),
      behavior: OneshotBehavior::new(),
    }
  }

  unsafe fn run(ptr: NonNull<()>) {
    let this = VPtr::get_ref::<Self>(ptr);
    let func = unsafe { ManuallyDrop::take(&mut (*this.function.get())) };
    this.behavior.fulfill_and_wake(func());
  }
}
impl<F, R> TaskPayload<F, R> {
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

pub fn into_task<F, R>(function: F) -> (TaskRef, PendingTask<R>)
where
  F: FnOnce() -> R + Send + 'static,
  R: Send + 'static,
{
  let task = TaskPayload::new(function);
  let (p1, p2) = VPtr::new_pair(task, &TaskPayload::<F, R>::VTABLE);
  (TaskRef::new(p1), PendingTask::new(p2))
}

struct TaskRefInner(VPtr);
impl TaskRefInner {
  unsafe fn run(&self) {
    let vtable = self.0.vtable();
    let ptr = self.0.erased();
    unsafe { (vtable.run)(ptr) };
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

pub struct TaskRef(ManuallyDrop<TaskRefInner>);
impl TaskRef {
  const fn new(ptr: VPtr) -> Self {
    Self(ManuallyDrop::new(TaskRefInner(ptr)))
  }
  pub fn run(self) {
    let mut this = ManuallyDrop::new(self);
    let inner = unsafe { ManuallyDrop::take(&mut this.0) };
    unsafe { inner.run() };
  }
}
impl Drop for TaskRef {
  fn drop(&mut self) {
    let inner = unsafe { ManuallyDrop::take(&mut self.0) };
    unsafe { inner.drop_in_place() };
  }
}

pub struct PendingTask<R> {
  task: VPtr,
  _marker: PhantomData<R>,
}
impl<R> PendingTask<R> {
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
impl<R> Drop for PendingTask<R> {
  fn drop(&mut self) {
    let vtable = self.task.vtable();
    let ptr = self.task.erased();
    unsafe { (vtable.drop_receiver)(ptr) };
  }
}
