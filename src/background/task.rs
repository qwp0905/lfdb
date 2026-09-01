use std::{cell::UnsafeCell, marker::PhantomData, mem::MaybeUninit, ptr::NonNull};

use super::{OneshotBehavior, VPtr as VPtrRaw, WaitDisconnectedError};

type WaitResult<T> = std::result::Result<T, WaitDisconnectedError>;

type VPtr = VPtrRaw<TaskVTable>;

unsafe fn run<F, R>(ptr: NonNull<()>)
where
  F: FnOnce() -> R + 'static,
  R: 'static,
{
  let task = VPtr::get_ref::<TaskPayload<F, R>>(ptr);
  task.run();
}

unsafe fn wait<F: 'static, R: 'static>(ptr: NonNull<()>, value: NonNull<()>) {
  let task = VPtr::get_ref::<TaskPayload<F, R>>(ptr);
  value.cast::<WaitResult<R>>().write(task.wait());
}

unsafe fn drop_sender<F: 'static, R: 'static>(ptr: NonNull<()>) {
  let task = VPtr::get_ref::<TaskPayload<F, R>>(ptr);
  task.drop_sender();
}
unsafe fn drop_receiver<F: 'static, R: 'static>(ptr: NonNull<()>) {
  let task = VPtr::get_ref::<TaskPayload<F, R>>(ptr);
  task.drop_receiver();
}

struct TaskVTable {
  run: unsafe fn(NonNull<()>),
  wait: unsafe fn(NonNull<()>, NonNull<()>),
  drop_sender: unsafe fn(NonNull<()>),
  drop_receiver: unsafe fn(NonNull<()>),
}

struct TaskPayload<F, R> {
  function: UnsafeCell<Option<F>>,
  behavior: OneshotBehavior<R>,
}
impl<F, R> TaskPayload<F, R>
where
  F: FnOnce() -> R + 'static,
  R: 'static,
{
  const VTABLE: TaskVTable = TaskVTable {
    run: run::<F, R>,
    wait: wait::<F, R>,
    drop_sender: drop_sender::<F, R>,
    drop_receiver: drop_receiver::<F, R>,
  };

  const fn new(function: F) -> Self {
    Self {
      function: UnsafeCell::new(Some(function)),
      behavior: OneshotBehavior::new(),
    }
  }

  unsafe fn run(&self) {
    let func = (*self.function.get()).take().unwrap();
    self.behavior.fulfill(func());
  }
}
impl<F, R> TaskPayload<F, R> {
  unsafe fn drop_receiver(&self) {
    self.behavior.drop_receiver();
  }
  unsafe fn drop_sender(&self) {
    self.behavior.drop_sender();
  }

  fn wait(&self) -> WaitResult<R> {
    self.behavior.wait()
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

pub struct TaskRef(VPtr);
impl TaskRef {
  const fn new(ptr: VPtr) -> Self {
    Self(ptr)
  }
  pub fn run(self) {
    let vtable = self.0.vtable();
    let ptr = self.0.erased();
    unsafe { (vtable.run)(ptr) };
  }
}
impl Drop for TaskRef {
  fn drop(&mut self) {
    let vtable = self.0.vtable();
    let ptr = self.0.erased();
    unsafe { (vtable.drop_sender)(ptr) };
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
    unsafe {
      let mut result = MaybeUninit::<WaitResult<R>>::uninit();
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
