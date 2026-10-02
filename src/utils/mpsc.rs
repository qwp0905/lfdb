use std::{
  cell::{RefCell, UnsafeCell},
  mem::MaybeUninit,
  ptr::{null_mut, NonNull},
  sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering},
};

use crossbeam::utils::{Backoff, CachePadded};

const LAP: usize = 32;
const BLOCK_CAP: usize = LAP - 1;
const SHIFT: usize = 1;
const HAS_NEXT: usize = 1;

struct Slot<T> {
  value: UnsafeCell<MaybeUninit<T>>,
  written: AtomicBool,
}
impl<T> Slot<T> {
  const fn uninit() -> Self {
    Self {
      value: UnsafeCell::new(MaybeUninit::uninit()),
      written: AtomicBool::new(false),
    }
  }

  fn write(&self, value: T) {
    unsafe { (*self.value.get()).write(value) };
    self.written.store(true, Ordering::Release);
  }

  fn read(&self) -> T {
    let backoff = Backoff::new();
    while !self.written.load(Ordering::Acquire) {
      backoff.snooze();
    }
    unsafe { (*self.value.get()).assume_init_read() }
  }

  fn drop_in_place(&mut self) {
    unsafe { self.value.get_mut().assume_init_drop() };
  }
}

struct Block<T> {
  next: AtomicPtr<Block<T>>,
  slots: [Slot<T>; BLOCK_CAP],
}
impl<T> Block<T> {
  const fn new() -> Self {
    Self {
      next: AtomicPtr::new(null_mut()),
      slots: [const { Slot::uninit() }; BLOCK_CAP],
    }
  }

  fn write(&self, value: T, index: usize) {
    self.slots[index].write(value);
  }

  fn read(&self, index: usize) -> T {
    self.slots[index].read()
  }

  fn wait_next(&self) -> NonNull<Self> {
    let backoff = Backoff::new();
    loop {
      let next = self.next.load(Ordering::Acquire);
      if let Some(next) = NonNull::new(next) {
        return next;
      }
      backoff.snooze();
    }
  }
}

struct Tail<T> {
  index: AtomicUsize,
  block: AtomicPtr<Block<T>>,
}
impl<T> Tail<T> {
  const fn new(ptr: *mut Block<T>, index: usize) -> Self {
    Self {
      index: AtomicUsize::new(index),
      block: AtomicPtr::new(ptr),
    }
  }
}
struct Head<T> {
  index: AtomicUsize,
  block: RefCell<NonNull<Block<T>>>,
}
impl<T> Head<T> {
  const fn new(ptr: *mut Block<T>, index: usize) -> Self {
    Self {
      index: AtomicUsize::new(index),
      block: RefCell::new(unsafe { NonNull::new_unchecked(ptr) }),
    }
  }
}
pub struct MpscQueue<T> {
  head: CachePadded<Head<T>>,
  tail: CachePadded<Tail<T>>,
}
impl<T> MpscQueue<T> {
  pub fn new() -> Self {
    let block = Box::new(Block::new());
    let ptr = Box::into_raw(block);
    Self {
      head: CachePadded::new(Head::new(ptr, 0)),
      tail: CachePadded::new(Tail::new(ptr, 0)),
    }
  }

  pub fn push(&self, value: T) {
    let backoff = Backoff::new();
    let mut tail = self.tail.index.load(Ordering::Acquire);
    let mut block = self.tail.block.load(Ordering::Acquire);
    let mut next_block = None;
    loop {
      debug_assert!(!block.is_null());

      let offset = (tail >> SHIFT) % LAP;

      if offset == BLOCK_CAP {
        backoff.snooze();
        tail = self.tail.index.load(Ordering::Acquire);
        block = self.tail.block.load(Ordering::Acquire);
        continue;
      }

      if offset + 1 == BLOCK_CAP && next_block.is_none() {
        next_block = Some(Box::new(Block::<T>::new()));
      }

      let new_tail = tail + (1 << SHIFT);
      match self.tail.index.compare_exchange_weak(
        tail,
        new_tail,
        Ordering::SeqCst,
        Ordering::Acquire,
      ) {
        Ok(_) => unsafe {
          if offset + 1 == BLOCK_CAP {
            let next_block = Box::into_raw(next_block.unwrap());
            let next_index = new_tail.wrapping_add(1 << SHIFT);

            self.tail.block.store(next_block, Ordering::Release);
            self.tail.index.store(next_index, Ordering::Release);
            (*block).next.store(next_block, Ordering::Release);
          }
          (*block).write(value, offset);
          return;
        },
        Err(t) => {
          tail = t;
          block = self.tail.block.load(Ordering::Acquire);
          backoff.spin();
        }
      }
    }
  }

  pub unsafe fn pop(&self) -> Option<T> {
    let mut block_ref = self.head.block.borrow_mut();

    let block = *block_ref;
    let head = unsafe { *self.head.index.as_ptr() };

    let offset = (head >> SHIFT) % LAP;
    let mut new_head = head + (1 << SHIFT);
    if new_head & HAS_NEXT == 0 {
      let tail = self.tail.index.load(Ordering::Acquire);
      if head >> SHIFT == tail >> SHIFT {
        return None;
      }

      if (head >> SHIFT) / LAP != (tail >> SHIFT) / LAP {
        new_head |= HAS_NEXT;
      }
    }

    unsafe {
      self.head.index.store(new_head, Ordering::Release);

      // If we've reached the end of the block, move to the next one.
      if offset + 1 == BLOCK_CAP {
        let next = block.as_ref().wait_next();
        let mut next_index = (new_head & !HAS_NEXT).wrapping_add(1 << SHIFT);
        if !next.as_ref().next.load(Ordering::Relaxed).is_null() {
          next_index |= HAS_NEXT;
        }

        *block_ref = next;
        self.head.index.store(next_index, Ordering::Release);
      }

      let value = block.as_ref().read(offset);
      if offset + 1 == BLOCK_CAP {
        let _ = Box::from_raw(block.as_ptr());
      }
      Some(value)
    }
  }

  pub fn len(&self) -> usize {
    loop {
      let mut tail = self.tail.index.load(Ordering::SeqCst);
      let mut head = self.head.index.load(Ordering::SeqCst);

      if self.tail.index.load(Ordering::SeqCst) == tail {
        tail &= !((1 << SHIFT) - 1);
        head &= !((1 << SHIFT) - 1);

        if (tail >> SHIFT) & (LAP - 1) == LAP - 1 {
          tail = tail.wrapping_add(1 << SHIFT);
        }
        if (head >> SHIFT) & (LAP - 1) == LAP - 1 {
          head = head.wrapping_add(1 << SHIFT);
        }

        let lap = (head >> SHIFT) / LAP;
        tail = tail.wrapping_sub((lap * LAP) << SHIFT);
        head = head.wrapping_sub((lap * LAP) << SHIFT);

        tail >>= SHIFT;
        head >>= SHIFT;

        return tail - head - tail / LAP;
      }
    }
  }

  pub fn is_empty(&self) -> bool {
    let head = self.head.index.load(Ordering::SeqCst);
    let tail = self.tail.index.load(Ordering::SeqCst);
    head >> SHIFT == tail >> SHIFT
  }
}

unsafe impl<T: Send> Send for MpscQueue<T> {}
unsafe impl<T: Send> Sync for MpscQueue<T> {}

impl<T> Drop for MpscQueue<T> {
  fn drop(&mut self) {
    let mut head = *self.head.index.get_mut();
    let mut tail = *self.tail.index.get_mut();
    let mut block = self.head.block.get_mut().as_ptr();

    head &= !((1 << SHIFT) - 1);
    tail &= !((1 << SHIFT) - 1);

    unsafe {
      while head != tail {
        let offset = (head >> SHIFT) % LAP;

        if offset < BLOCK_CAP {
          (*block).slots[offset].drop_in_place()
        } else {
          block = *Box::from_raw(block).next.get_mut();
        }
        head = head.wrapping_add(1 << SHIFT);
      }

      if !block.is_null() {
        let _ = Box::from_raw(block);
      }
    }
  }
}

#[cfg(test)]
#[path = "tests/mpsc.rs"]
mod tests;
