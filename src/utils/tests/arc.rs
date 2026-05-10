use crossbeam::channel::bounded;

use super::*;
use std::{cell::Cell, sync::Arc};

struct T {
  c: Arc<Cell<bool>>,
}
impl T {
  fn new(c: Arc<Cell<bool>>) -> Self {
    Self { c }
  }
}
impl Drop for T {
  fn drop(&mut self) {
    self.c.set(true)
  }
}
unsafe impl Send for T {}
unsafe impl Sync for T {}

#[test]
fn test_drop() {
  let c = Arc::new(Cell::new(false));
  {
    let a = SArc::new(T::new(c.clone()));
    let _ = a.clone();
    let _ = a.clone();
  }

  assert!(c.get())
}

#[test]
fn test_drop_with_move() {
  let c = Arc::new(Cell::new(false));
  let (t, r) = bounded::<()>(1);
  let th = {
    let a = SArc::new(T::new(c.clone()));
    let b = a.clone();
    let th = std::thread::spawn(move || {
      let _ = b.clone();
      r.recv().unwrap();
      let _ = b.clone();
    });
    let _ = a.clone();
    let _ = a.clone();
    th
  };
  assert!(!c.get());
  t.send(()).unwrap();
  th.join().unwrap();

  assert!(c.get())
}
