use loom::{
  cell::UnsafeCell,
  sync::{atomic::AtomicUsize, Arc},
  thread,
};

use super::*;

struct Payload {
  value: UnsafeCell<usize>,
  dropped: Arc<AtomicUsize>,
}
impl Payload {
  fn write(&self) {
    self.value.with_mut(|v| unsafe { *v += 1 });
  }
}
impl Drop for Payload {
  fn drop(&mut self) {
    self.value.with(|v| unsafe { *v });
    self.dropped.fetch_add(1, Ordering::Relaxed);
  }
}
unsafe impl Sync for Payload {}

fn new_payload() -> (TwBox<Payload>, TwBox<Payload>, Arc<AtomicUsize>) {
  let dropped = Arc::new(AtomicUsize::new(0));
  let payload = Payload {
    value: UnsafeCell::new(0),
    dropped: dropped.clone(),
  };
  let (a, b) = TwBox::new(payload);
  (a, b, dropped)
}

#[test]
fn concurrent_drops_release_once() {
  loom::model(|| {
    let (twbox, other, dropped) = new_payload();
    let handle = thread::spawn(move || drop(other));

    drop(twbox);
    handle.join().unwrap();

    assert_eq!(dropped.load(Ordering::Relaxed), 1);
  });
}

#[test]
fn writes_before_drop_are_visible_to_last_drop() {
  loom::model(|| {
    let (twbox, other, dropped) = new_payload();
    let handle = thread::spawn(move || {
      other.write();
      drop(other);
    });

    drop(twbox);
    handle.join().unwrap();

    assert_eq!(dropped.load(Ordering::Relaxed), 1);
  });
}
