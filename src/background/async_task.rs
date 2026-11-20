use std::{
  cell::UnsafeCell,
  convert::identity,
  marker::PhantomData,
  mem::{ManuallyDrop, MaybeUninit},
  ptr::NonNull,
};

use crate::utils::loom::thread::{park, yield_now};

use super::{Oneshot, Selectable, TryWaitError, VPtr as VPtrRaw, WaitDisconnectedError};

type VPtr = VPtrRaw<TaskVTable>;

struct TaskVTable {
  fulfill: unsafe fn(NonNull<()>, NonNull<()>),
  try_wait: unsafe fn(NonNull<()>, NonNull<()>),
  try_park: unsafe fn(NonNull<()>) -> bool,
  context: unsafe fn(NonNull<()>) -> NonNull<()>,
  drop_sender: unsafe fn(NonNull<()>),
  drop_receiver: unsafe fn(NonNull<()>),
  drop_in_place: unsafe fn(NonNull<()>),
}

struct TaskPayload<T, R, F, C> {
  function: UnsafeCell<ManuallyDrop<F>>,
  oneshot: Oneshot<R>,
  context: C,
  _marker: PhantomData<fn(T) -> R>,
}
impl<T, R, F, C> TaskPayload<T, R, F, C>
where
  F: FnOnce(T) -> R,
{
  const VTABLE: TaskVTable = TaskVTable {
    fulfill: Self::fulfill,
    try_wait: Self::try_wait,
    try_park: Self::try_park,
    drop_sender: Self::drop_sender,
    drop_receiver: Self::drop_receiver,
    drop_in_place: Self::drop_in_place,
    context: Self::context,
  };

  fn new(function: F, context: C) -> Self {
    Self {
      function: UnsafeCell::new(ManuallyDrop::new(function)),
      oneshot: Oneshot::new(),
      context,
      _marker: PhantomData,
    }
  }

  unsafe fn fulfill(ptr: NonNull<()>, value: NonNull<()>) {
    let this = VPtr::get_ref::<Self>(ptr);
    let func = unsafe { ManuallyDrop::take(&mut (*this.function.get())) };
    let value = unsafe { value.cast::<T>().read() };
    unsafe { this.oneshot.fulfill_and_wake(func(value)) };
  }
}
impl<T, R, F, C> TaskPayload<T, R, F, C> {
  unsafe fn drop_receiver(ptr: NonNull<()>) {
    let this = VPtr::get_ref::<Self>(ptr);
    this.oneshot.drop_receiver();
  }
  unsafe fn drop_sender(ptr: NonNull<()>) {
    let this = VPtr::get_ref::<Self>(ptr);
    this.oneshot.drop_sender();
  }
  unsafe fn try_wait(ptr: NonNull<()>, value: NonNull<()>) {
    let this = VPtr::get_ref::<Self>(ptr);
    let casted = value.cast::<Result<R, TryWaitError<()>>>();
    unsafe { casted.write(this.oneshot.try_wait()) };
  }
  unsafe fn try_park(ptr: NonNull<()>) -> bool {
    let this = VPtr::get_ref::<Self>(ptr);
    unsafe { this.oneshot.try_park() }
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
// SAFETY: the two handles of a pair touch disjoint parts of the payload. Only the
// fulfiller takes `function` and reads `context`, only the receiver calls the
// receiver side of `oneshot`, and `oneshot` synchronizes the hand-off between them.
unsafe impl<T, R: Send, F: Send, C: Send> Sync for TaskPayload<T, R, F, C> {}

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
  pub const unsafe fn from_raw(raw: *mut ()) -> Self {
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

  pub fn try_wait(self) -> Result<R, TryWaitError<Self>> {
    let vtable = self.task.vtable();
    let ptr = self.task.erased();
    let mut result = MaybeUninit::<Result<R, TryWaitError<()>>>::uninit();
    let result_ptr = unsafe { NonNull::new_unchecked(result.as_mut_ptr()) };
    unsafe { (vtable.try_wait)(ptr, result_ptr.cast()) };
    match unsafe { result.assume_init() } {
      Ok(v) => Ok(v),
      Err(TryWaitError::Disconnected) => Err(TryWaitError::Disconnected),
      Err(TryWaitError::Empty(_)) => Err(TryWaitError::Empty(self)),
    }
  }

  pub fn try_park(&self) -> bool {
    let vtable = self.task.vtable();
    let ptr = self.task.erased();
    unsafe { (vtable.try_park)(ptr) }
  }

  #[cfg(not(loom))]
  const MAX_YIELD: u8 = 10;
  // Loom keeps scheduling the other thread while one yields, so a long yield
  // phase lets the sender always finish before the waiter reaches park.
  #[cfg(loom)]
  const MAX_YIELD: u8 = 1;
  pub fn wait(mut self) -> Result<R, WaitDisconnectedError> {
    loop {
      for _ in 0..Self::MAX_YIELD {
        match self.try_wait() {
          Ok(v) => return Ok(v),
          Err(TryWaitError::Disconnected) => return Err(WaitDisconnectedError),
          Err(TryWaitError::Empty(this)) => self = this,
        };
        yield_now();
      }
      if self.try_park() {
        park();
      }
    }
  }
}
impl<R> Selectable for AsyncTask<R> {
  type Output = R;
  fn try_wait(self) -> Result<R, TryWaitError<Self>> {
    AsyncTask::try_wait(self)
  }
  fn try_park(&self) -> bool {
    AsyncTask::try_park(self)
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

#[cfg(all(test, loom))]
#[path = "tests/async_task_loom.rs"]
mod tests;
