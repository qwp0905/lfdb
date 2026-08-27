use std::{
  cell::UnsafeCell,
  marker::PhantomData,
  mem::MaybeUninit,
  ptr::NonNull,
  sync::atomic::{fence, AtomicBool, Ordering},
  thread::{current, park, Thread},
};

use crossbeam::atomic::AtomicCell;

use super::{TryWaitError, WaitDisconnectedError};

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
  Waiting,
  Fulfilled,
  Disconnected,
}

enum WaitResult<T> {
  Done(T),
  Empty,
  Disconnected,
}

unsafe fn run<F, R>(ptr: NonNull<TaskHeader>)
where
  F: FnOnce() -> R,
{
  let task = ptr.cast::<Task<F, R>>().as_ref();
  task.run();
}

unsafe fn try_wait<F, R>(ptr: NonNull<TaskHeader>, value: NonNull<()>) {
  let task = ptr.cast::<Task<F, R>>().as_ref();
  value.cast::<WaitResult<R>>().write(task.try_wait());
}

unsafe fn drop_inner<F, R>(ptr: NonNull<TaskHeader>) {
  let raw = ptr.cast::<Task<F, R>>();
  if !raw.as_ref().try_drop() {
    return;
  }
  let _ = Box::from_raw(raw.as_ptr());
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
  try_wait: unsafe fn(NonNull<TaskHeader>, NonNull<()>),
  drop_inner: unsafe fn(NonNull<TaskHeader>),
  drop_sender: unsafe fn(NonNull<TaskHeader>),
  drop_receiver: unsafe fn(NonNull<TaskHeader>),
}

struct TaskHeader {
  state: AtomicCell<State>,
  caller: AtomicCell<Option<Thread>>,
  dropped: AtomicBool,
}
impl TaskHeader {
  fn set_caller(&self) {
    self.caller.store(Some(current()));
  }
  const fn new() -> Self {
    Self {
      state: AtomicCell::new(State::Waiting),
      caller: AtomicCell::new(None),
      dropped: AtomicBool::new(false),
    }
  }
}

struct TaskPayload<F, R> {
  function: UnsafeCell<Option<F>>,
  result: UnsafeCell<MaybeUninit<R>>,
}
impl<F, R> TaskPayload<F, R> {
  const fn new(function: F) -> Self {
    Self {
      function: UnsafeCell::new(Some(function)),
      result: UnsafeCell::new(MaybeUninit::uninit()),
    }
  }
  const unsafe fn fulfill(&self, value: R) {
    (*self.result.get()).write(value);
  }
  const unsafe fn take(&self) -> F {
    (*self.function.get()).take().unwrap()
  }
  unsafe fn drop_in_place(&self) {
    (*self.result.get()).assume_init_drop();
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
    try_wait: try_wait::<F, R>,
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
    let func = self.payload.take();
    self.payload.fulfill(func());

    match self
      .header
      .state
      .compare_exchange(State::Waiting, State::Fulfilled)
      .unwrap_or_else(|s| s)
    {
      State::Waiting => {
        let Some(thread) = self.header.caller.take() else {
          return;
        };
        thread.unpark();
      }
      State::Fulfilled => unreachable!(),
      State::Disconnected => self.payload.drop_in_place(),
    }
  }
}
impl<F, R> Task<F, R> {
  fn try_drop(&self) -> bool {
    if !self.header.dropped.fetch_or(true, Ordering::Release) {
      return false;
    };
    fence(Ordering::Acquire);
    true
  }

  fn try_wait(&self) -> WaitResult<R> {
    match self
      .header
      .state
      .compare_exchange(State::Fulfilled, State::Disconnected)
      .unwrap_or_else(|s| s)
    {
      State::Waiting => WaitResult::Empty,
      State::Fulfilled => {
        let result = unsafe { (*self.payload.result.get()).assume_init_read() };
        WaitResult::Done(result)
      }
      State::Disconnected => WaitResult::Disconnected,
    }
  }

  unsafe fn drop_receiver(&self) {
    if let State::Fulfilled = self.header.state.swap(State::Disconnected) {
      self.payload.drop_in_place();
    }
  }
  unsafe fn drop_sender(&self) {
    if self
      .header
      .state
      .compare_exchange(State::Waiting, State::Disconnected)
      .is_err()
    {
      return;
    };
    let Some(thread) = self.header.caller.take() else {
      return;
    };
    thread.unpark();
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
  F: FnOnce() -> R,
{
  let task = Box::new(Task::new(function));
  let vtable = &Task::<F, R>::VTABLE;
  let ptr = TaskPtr::new(NonNull::from_mut(Box::leak(task)).cast(), vtable);
  (TaskRef::new(ptr.clone()), PendingTask::new(ptr))
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

  pub fn try_wait(self) -> std::result::Result<R, TryWaitError<Self>> {
    unsafe {
      let mut result = MaybeUninit::<WaitResult<R>>::uninit();
      let result_ptr = NonNull::new_unchecked(result.as_mut_ptr().cast());
      (self.task.vtable.try_wait)(self.task.ptr, result_ptr);
      match result.assume_init_read() {
        WaitResult::Done(v) => Ok(v),
        WaitResult::Empty => Err(TryWaitError::Empty(self)),
        WaitResult::Disconnected => Err(TryWaitError::Disconnected),
      }
    }
  }

  pub fn wait_slow(self) -> std::result::Result<R, WaitDisconnectedError> {
    unsafe {
      let mut result = MaybeUninit::<WaitResult<R>>::uninit();
      let result_ptr = NonNull::new_unchecked(result.as_mut_ptr().cast());
      self.task.ptr.as_ref().set_caller();
      loop {
        (self.task.vtable.try_wait)(self.task.ptr, result_ptr);
        match result.assume_init_read() {
          WaitResult::Done(v) => return Ok(v),
          WaitResult::Disconnected => return Err(WaitDisconnectedError),
          WaitResult::Empty => park(),
        };
      }
    }
  }
}
impl<R> Drop for PendingTask<R> {
  fn drop(&mut self) {
    unsafe { (self.task.vtable.drop_receiver)(self.task.ptr) };
  }
}
