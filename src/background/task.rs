use std::{cell::UnsafeCell, marker::PhantomData, mem::MaybeUninit, ptr::NonNull};

use super::{OneshotBehavior, Pair, WaitDisconnectedError};

type WaitResult<T> = std::result::Result<T, WaitDisconnectedError>;

unsafe fn run<F, R>(ptr: NonNull<TaskHeader>)
where
  F: FnOnce() -> R,
{
  let task = ptr.cast::<Task<F, R>>().as_ref();
  task.run();
}

unsafe fn wait<F, R>(ptr: NonNull<TaskHeader>, value: NonNull<()>) {
  let task = ptr.cast::<Task<F, R>>().as_ref();
  value.cast::<WaitResult<R>>().write(task.wait());
}

unsafe fn drop_inner<F, R>(ptr: NonNull<TaskHeader>) {
  let raw = ptr.cast::<Task<F, R>>();
  let _ = Pair::from_raw(raw.as_ptr());
}

unsafe fn drop_sender<F, R>(ptr: NonNull<TaskHeader>) {
  let task = ptr.cast::<Task<F, R>>().as_ref();
  task.drop_sender();
}
unsafe fn drop_receiver<F, R>(ptr: NonNull<TaskHeader>) {
  let task = ptr.cast::<Task<F, R>>().as_ref();
  task.drop_receiver();
}

struct TaskVTable {
  run: unsafe fn(NonNull<TaskHeader>),
  wait: unsafe fn(NonNull<TaskHeader>, NonNull<()>),
  drop_inner: unsafe fn(NonNull<TaskHeader>),
  drop_sender: unsafe fn(NonNull<TaskHeader>),
  drop_receiver: unsafe fn(NonNull<TaskHeader>),
}

struct TaskHeader {}
impl TaskHeader {
  const fn new() -> Self {
    Self {}
  }
}

struct TaskPayload<F, R> {
  function: UnsafeCell<Option<F>>,
  behavior: OneshotBehavior<R>,
}
impl<F, R> TaskPayload<F, R> {
  const fn new(function: F) -> Self {
    Self {
      function: UnsafeCell::new(Some(function)),
      behavior: OneshotBehavior::new(),
    }
  }
}

#[repr(C)]
pub struct Task<F, R> {
  header: TaskHeader,
  payload: TaskPayload<F, R>,
}
impl<F, R> Task<F, R>
where
  F: FnOnce() -> R,
{
  const VTABLE: TaskVTable = TaskVTable {
    run: run::<F, R>,
    wait: wait::<F, R>,
    drop_inner: drop_inner::<F, R>,
    drop_sender: drop_sender::<F, R>,
    drop_receiver: drop_receiver::<F, R>,
  };

  const fn new(function: F) -> Self {
    Self {
      header: TaskHeader::new(),
      payload: TaskPayload::new(function),
    }
  }

  unsafe fn run(&self) {
    let func = (*self.payload.function.get()).take().unwrap();
    self.payload.behavior.fulfill(func());
  }
}
impl<F, R> Task<F, R> {
  unsafe fn drop_receiver(&self) {
    self.payload.behavior.drop_receiver();
  }
  unsafe fn drop_sender(&self) {
    self.payload.behavior.drop_sender();
  }

  fn wait(&self) -> WaitResult<R> {
    self.payload.behavior.wait()
  }
}
#[derive(Clone)]
struct TaskPtr {
  ptr: NonNull<TaskHeader>,
  vtable: &'static TaskVTable,
}
impl TaskPtr {
  const fn new(ptr: NonNull<TaskHeader>, vtable: &'static TaskVTable) -> Self {
    Self { ptr, vtable }
  }
}
impl Drop for TaskPtr {
  fn drop(&mut self) {
    unsafe { (self.vtable.drop_inner)(self.ptr) }
  }
}
unsafe impl Send for TaskPtr {}
unsafe impl Sync for TaskPtr {}

pub fn into_task<F, R>(function: F) -> (TaskRef, PendingTask<R>)
where
  F: FnOnce() -> R + 'static,
{
  let task = Task::new(function);
  let vtable = &Task::<F, R>::VTABLE;
  let (p1, p2) = Pair::new(task);

  let p1 = unsafe { NonNull::new_unchecked(Pair::into_raw(p1)) };
  let p2 = unsafe { NonNull::new_unchecked(Pair::into_raw(p2)) };
  let p1 = TaskPtr::new(p1.cast(), vtable);
  let p2 = TaskPtr::new(p2.cast(), vtable);
  (TaskRef::new(p1), PendingTask::new(p2))
}

pub struct TaskRef(TaskPtr);
impl TaskRef {
  const fn new(ptr: TaskPtr) -> Self {
    Self(ptr)
  }
  pub fn run(self) {
    unsafe { (self.0.vtable.run)(self.0.ptr) };
  }
}
impl Drop for TaskRef {
  fn drop(&mut self) {
    unsafe { (self.0.vtable.drop_sender)(self.0.ptr) };
  }
}

pub struct PendingTask<R> {
  task: TaskPtr,
  _marker: PhantomData<R>,
}
impl<R> PendingTask<R> {
  const fn new(task: TaskPtr) -> Self {
    Self {
      task,
      _marker: PhantomData,
    }
  }

  pub fn wait(self) -> WaitResult<R> {
    unsafe {
      let mut result = MaybeUninit::<WaitResult<R>>::uninit();
      let result_ptr = NonNull::new_unchecked(result.as_mut_ptr().cast());
      (self.task.vtable.wait)(self.task.ptr, result_ptr);
      result.assume_init()
    }
  }
}
impl<R> Drop for PendingTask<R> {
  fn drop(&mut self) {
    unsafe { (self.task.vtable.drop_receiver)(self.task.ptr) };
  }
}
