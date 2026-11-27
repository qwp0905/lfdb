use std::{
  cell::UnsafeCell,
  ops::Deref,
  ptr::NonNull,
  sync::atomic::{fence, AtomicBool, Ordering},
};

use crossbeam::epoch::{pin, unprotected, Atomic, Guard, Owned, Shared};

struct Node<T> {
  data: UnsafeCell<Option<T>>,
  next: Atomic<Node<T>>,
  released: AtomicBool,
}
impl<T> Node<T> {
  const fn new(value: T) -> Self {
    Self {
      data: UnsafeCell::new(Some(value)),
      next: Atomic::null(),
      released: AtomicBool::new(false),
    }
  }
  const fn dummy() -> Self {
    Self {
      data: UnsafeCell::new(None),
      next: Atomic::null(),
      released: AtomicBool::new(true),
    }
  }
}

unsafe fn drop_node<T>(node: *const Node<T>) {
  if !(*node).released.fetch_or(true, Ordering::Release) {
    return;
  }
  fence(Ordering::Acquire);
  let _ = unsafe { Shared::from(node).into_owned() };
}

pub struct MsQueue<T> {
  head: Atomic<Node<T>>,
  tail: Atomic<Node<T>>,
}
impl<T> MsQueue<T> {
  pub fn new() -> Self {
    let node = Owned::new(Node::dummy()).into_shared(unsafe { unprotected() });
    let head = Atomic::from(node);
    let tail = Atomic::from(node);
    Self { head, tail }
  }

  #[cfg(all(test, not(loom)))]
  pub fn push(&self, value: T) {
    unsafe { self.push_then(value) };
  }

  pub unsafe fn push_then(&self, value: T) -> PushGuard<T> {
    let mut new = Owned::new(Node::new(value));
    let guard = pin();
    let mut tail = self.tail.load(Ordering::Acquire, &guard);
    loop {
      let tail_ref = unsafe { tail.deref() };
      let next = tail_ref.next.load(Ordering::Acquire, &guard);
      if !next.is_null() {
        match self.tail.compare_exchange_weak(
          tail,
          next,
          Ordering::Release,
          Ordering::Acquire,
          &guard,
        ) {
          Ok(_) => tail = next,
          Err(err) => tail = err.current,
        }
        continue;
      }

      match tail_ref.next.compare_exchange_weak(
        Shared::null(),
        new,
        Ordering::Release,
        Ordering::Relaxed,
        &guard,
      ) {
        Ok(new) => {
          let _ = self.tail.compare_exchange_weak(
            tail,
            new,
            Ordering::Release,
            Ordering::Relaxed,
            &guard,
          );
          return PushGuard::new(unsafe { &*new.as_raw() });
        }
        Err(err) => new = err.new,
      }
    }
  }

  pub fn pop_then(&self) -> Option<PopGuard<T>> {
    let guard = pin();
    let mut head = self.head.load(Ordering::Acquire, &guard);
    loop {
      let next = unsafe { head.deref() }.next.load(Ordering::Acquire, &guard);
      if next.is_null() {
        return None;
      }

      let tail = self.tail.load(Ordering::Relaxed, &guard);
      if tail == head {
        let _ = self.tail.compare_exchange(
          tail,
          next,
          Ordering::Release,
          Ordering::Relaxed,
          &guard,
        );
      }
      if let Err(err) = self.head.compare_exchange_weak(
        head,
        next,
        Ordering::Release,
        Ordering::Acquire,
        &guard,
      ) {
        head = err.current;
        continue;
      }

      let head = head.as_raw();
      unsafe { guard.defer_unchecked(move || drop_node(head)) };
      return Some(PopGuard::new(unsafe { &*next.as_raw() }, guard));
    }
  }

  #[cfg(all(test, not(loom)))]
  pub fn pop(&self) -> Option<T> {
    let guard = self.pop_then()?;
    Some(unsafe { (*guard.node.as_ref().data.get()).take().unwrap() })
  }
}
impl<T> Drop for MsQueue<T> {
  fn drop(&mut self) {
    let guard = unsafe { unprotected() };
    let mut node = self.head.swap(Shared::null(), Ordering::Relaxed, guard);
    while let Some(node_ref) = unsafe { node.as_ref() } {
      let next = node_ref.next.load(Ordering::Relaxed, guard);
      let ptr = node.as_raw();
      unsafe { guard.defer_unchecked(move || drop_node(ptr)) };
      node = next;
    }
  }
}
impl<T> Default for MsQueue<T> {
  fn default() -> Self {
    Self::new()
  }
}

pub struct PushGuard<T> {
  node: NonNull<Node<T>>,
}
impl<T> PushGuard<T> {
  const fn new(node: &Node<T>) -> Self {
    Self {
      node: NonNull::from_ref(node),
    }
  }
}
impl<T> Drop for PushGuard<T> {
  fn drop(&mut self) {
    unsafe { drop_node(self.node.as_ptr()) };
  }
}

pub struct PopGuard<T> {
  node: NonNull<Node<T>>,
  _guard: Guard,
}
impl<T> PopGuard<T> {
  const fn new(data: &Node<T>, _guard: Guard) -> Self {
    Self {
      node: NonNull::from_ref(data),
      _guard,
    }
  }
}
impl<T> Deref for PushGuard<T> {
  type Target = T;
  fn deref(&self) -> &Self::Target {
    unsafe { (*self.node.as_ref().data.get()).as_ref().unwrap() }
  }
}
impl<T> Deref for PopGuard<T> {
  type Target = T;
  fn deref(&self) -> &Self::Target {
    unsafe { (*self.node.as_ref().data.get()).as_ref().unwrap() }
  }
}

unsafe impl<T: Send> Send for MsQueue<T> {}
unsafe impl<T: Send + Sync> Sync for MsQueue<T> {}

#[cfg(all(test, not(loom)))]
#[path = "tests/ms_queue.rs"]
mod tests;
