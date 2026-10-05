use crossbeam::channel::bounded;

use super::*;
use std::cell::Cell;

struct T {
  c: SBox<Cell<bool>>,
}
impl T {
  fn new(c: SBox<Cell<bool>>) -> Self {
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

struct C {
  c: SBox<AtomicUsize>,
}
impl C {
  fn new(c: SBox<AtomicUsize>) -> Self {
    Self { c }
  }
}
impl Drop for C {
  fn drop(&mut self) {
    self.c.fetch_add(1, Ordering::Relaxed);
  }
}
unsafe impl Send for C {}
unsafe impl Sync for C {}

#[test]
fn test_drop() {
  let c = SBox::new(Cell::new(false));
  {
    let a = SBox::new(T::new(c.clone()));
    let _ = a.clone();
    let _ = a.clone();
  }

  assert!(c.get())
}

#[test]
fn test_drop_with_move() {
  let c = SBox::new(Cell::new(false));
  let (t, r) = bounded::<()>(1);
  let th = {
    let a = SBox::new(T::new(c.clone()));
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

#[test]
fn test_into_raw_from_raw_drop_counts() {
  let c = SBox::new(AtomicUsize::new(0));
  let a = SBox::new(C::new(c.clone()));
  let mut raw: Vec<_> = (0..4).map(|_| SBox::into_raw(a.clone())).collect();

  drop(a);
  assert_eq!(c.load(Ordering::Relaxed), 0);

  while let Some(ptr) = raw.pop() {
    // SAFETY: Each into_raw transfers one owned reference, restored exactly once.
    let restored = unsafe { SBox::from_raw(ptr) };
    assert_eq!(c.load(Ordering::Relaxed), 0);

    drop(restored);
    assert_eq!(c.load(Ordering::Relaxed), usize::from(raw.is_empty()));
  }
}
