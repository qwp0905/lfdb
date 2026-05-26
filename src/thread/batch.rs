use std::{
  panic::{RefUnwindSafe, UnwindSafe},
  sync::atomic::{AtomicBool, Ordering},
};

use crossbeam::queue::SegQueue;

use crate::thread::{oneshot, Oneshot, OneshotFulfill};

pub struct BatchExecution<T, R> {
  handler: Box<dyn Fn(Vec<T>) -> R>,
  queue: SegQueue<(T, OneshotFulfill<R>)>,
  occupied: AtomicBool,
  max_count: usize,
}
impl<T, R: Clone> BatchExecution<T, R> {
  pub fn new<F>(handler: F, max_count: usize) -> Self
  where
    F: Fn(Vec<T>) -> R + 'static,
  {
    Self {
      handler: Box::new(handler),
      queue: SegQueue::new(),
      occupied: AtomicBool::new(false),
      max_count,
    }
  }

  fn flush(&self, buffered: &mut Vec<(T, OneshotFulfill<R>)>) {
    if buffered.is_empty() {
      return;
    }

    let (value, waiting): (Vec<_>, Vec<_>) = buffered.drain(..).unzip();
    let result = (&self.handler)(value);

    waiting
      .into_iter()
      .for_each(|done| done.fulfill(result.clone()));
  }

  pub fn execute(&self, v: T) -> Oneshot<R> {
    let (o, f) = oneshot();

    self.queue.push((v, f));
    if self.occupied.fetch_or(true, Ordering::Release) {
      return o;
    }

    let mut buffered = Vec::with_capacity(self.max_count);
    loop {
      for task in (0..self.max_count).map_while(|_| self.queue.pop()) {
        buffered.push(task);
      }

      self.flush(&mut buffered);
      self.occupied.fetch_and(false, Ordering::Release);
      if self.queue.is_empty() {
        break;
      }

      if self.occupied.fetch_or(true, Ordering::AcqRel) {
        break;
      }
    }

    o
  }
}
unsafe impl<T: Send, R: Send> Send for BatchExecution<T, R> {}
unsafe impl<T: Sync, R: Sync> Sync for BatchExecution<T, R> {}
impl<T, R> RefUnwindSafe for BatchExecution<T, R> {}
impl<T, R> UnwindSafe for BatchExecution<T, R> {}
