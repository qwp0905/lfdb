use std::{
  cell::{OnceCell, RefCell},
  ops::{Deref, DerefMut},
  sync::{
    atomic::{AtomicU8, Ordering},
    Arc, Weak,
  },
  thread::{current, park, LocalKey, Thread},
};

use crossbeam::{
  queue::{ArrayQueue, SegQueue},
  utils::Backoff,
};

use crate::utils::SBox;

use super::{AlignedBuf, Page};

macro_rules! create_page_allocator {
  ($capacity:expr $(,)?) => {{
    use crate::page::{LocalRef, PageAllocator};
    thread_local! {
      static LOCAL: LocalRef = const { LocalRef::new() };
    }
    PageAllocator::new($capacity, &LOCAL)
  }};
}
pub(crate) use create_page_allocator;

pub struct PageRef<const N: usize> {
  page: Page<N>,
  global: Arc<GlobalQueue>,
}
impl<const N: usize> PageRef<N> {
  const fn new(ptr: *mut u8, global: Arc<GlobalQueue>) -> Self {
    Self {
      page: Page::new(ptr),
      global,
    }
  }
}
impl<const N: usize> Drop for PageRef<N> {
  fn drop(&mut self) {
    self.global.store(self.page.as_ptr())
  }
}
impl<const N: usize> Deref for PageRef<N> {
  type Target = Page<N>;
  fn deref(&self) -> &Self::Target {
    &self.page
  }
}
impl<const N: usize> DerefMut for PageRef<N> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.page
  }
}

pub struct PageAllocator<const N: usize> {
  global: Arc<GlobalQueue>,
  local_key: &'static LocalKey<LocalRef>,
}
impl<const N: usize> PageAllocator<N> {
  pub fn new(capacity: usize, local_key: &'static LocalKey<LocalRef>) -> Self {
    Self {
      global: Arc::new(GlobalQueue::new(capacity, N)),
      local_key,
    }
  }

  fn create_with(&self, ptr: *mut u8) -> PageRef<N> {
    PageRef::new(ptr, self.global.clone())
  }

  fn create_local(&self) -> LocalQueue {
    LocalQueue::new(Arc::downgrade(&self.global))
  }

  pub fn allocate(&self) -> PageRef<N> {
    self.local_key.with(|v| {
      let mut local = v
        .get_or_init(|| RefCell::new(self.create_local()))
        .borrow_mut();
      if local.global.as_ptr() != local.global.as_ptr() {
        *local = self.create_local();
      }
      let ptr = self.global.pop_with(&local.waker);
      self.create_with(ptr)
    })
  }
}

struct GlobalQueue {
  free: ArrayQueue<usize>,
  wakers: WakeQueue,
  buf: AlignedBuf,
}
impl GlobalQueue {
  fn new(capacity: usize, page_size: usize) -> Self {
    let buf = AlignedBuf::new(capacity * page_size);
    let free = ArrayQueue::new(capacity);
    let mut offset = 0;
    while offset < buf.len() {
      let _ = free.push(offset);
      offset += page_size;
    }
    Self {
      free,
      buf,
      wakers: WakeQueue::new(),
    }
  }

  fn store(&self, ptr: *mut u8) {
    let offset = unsafe { ptr.offset_from_unsigned(self.buf.as_ptr()) };
    let _ = self.free.push(offset);
    self.wakers.wake_once();
  }

  fn pop_with(&self, waker: &SBox<Waker>) -> *mut u8 {
    let backoff = Backoff::new();
    loop {
      while !backoff.is_completed() {
        if let Some(offset) = self.free.pop() {
          return unsafe { self.buf.as_ptr().add(offset) };
        }
        backoff.snooze();
      }

      self.wakers.try_enqueue(waker);
      let Some(offset) = self.free.pop() else {
        waker.try_park();
        backoff.reset();
        continue;
      };
      if !waker.try_cancel() {
        self.wakers.wake_once();
      }
      return unsafe { self.buf.as_ptr().add(offset) };
    }
  }
}
unsafe impl Send for GlobalQueue {}
unsafe impl Sync for GlobalQueue {}

const STATE_UNQUEUED: u8 = 0;
const STATE_QUEUED: u8 = 1;
const STATE_PARKED: u8 = 2;
const STATE_CANCELED: u8 = 3;

struct Waker {
  state: AtomicU8,
  thread: Thread,
}
impl Waker {
  fn new() -> Self {
    Self {
      state: AtomicU8::new(STATE_UNQUEUED),
      thread: current(),
    }
  }

  fn try_park(&self) {
    if self
      .state
      .compare_exchange(
        STATE_QUEUED,
        STATE_PARKED,
        Ordering::AcqRel,
        Ordering::Acquire,
      )
      .is_err()
    {
      return;
    }

    debug_assert_eq!(current().id(), self.thread.id());
    park();
    let _ = self.state.compare_exchange(
      STATE_PARKED,
      STATE_QUEUED,
      Ordering::Release,
      Ordering::Acquire,
    );
  }

  fn try_cancel(&self) -> bool {
    self
      .state
      .compare_exchange(
        STATE_QUEUED,
        STATE_CANCELED,
        Ordering::AcqRel,
        Ordering::Acquire,
      )
      .is_ok()
  }
}

struct WakeQueue {
  queue: SegQueue<SBox<Waker>>,
}
impl WakeQueue {
  const fn new() -> Self {
    Self {
      queue: SegQueue::new(),
    }
  }
  fn try_enqueue(&self, waker: &SBox<Waker>) {
    match waker.state.swap(STATE_QUEUED, Ordering::AcqRel) {
      STATE_UNQUEUED => self.queue.push(waker.clone()),
      STATE_QUEUED | STATE_CANCELED => {}
      _ => unreachable!(),
    }
  }

  fn wake_once(&self) {
    while let Some(waker) = self.queue.pop() {
      let state = waker.state.swap(STATE_UNQUEUED, Ordering::AcqRel);
      match state {
        STATE_QUEUED => return,
        STATE_PARKED => return waker.thread.unpark(),
        STATE_CANCELED => continue,
        _ => unreachable!(),
      }
    }
  }
}

pub type LocalRef = OnceCell<RefCell<LocalQueue>>;

pub struct LocalQueue {
  global: Weak<GlobalQueue>,
  waker: SBox<Waker>,
}
impl LocalQueue {
  fn new(global: Weak<GlobalQueue>) -> Self {
    Self {
      global,
      waker: SBox::new(Waker::new()),
    }
  }
}
