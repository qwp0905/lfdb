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

fn new_payload() -> (SBox<Payload>, Arc<AtomicUsize>) {
  let dropped = Arc::new(AtomicUsize::new(0));
  let payload = Payload {
    value: UnsafeCell::new(0),
    dropped: dropped.clone(),
  };
  (SBox::new(payload), dropped)
}

#[test]
fn concurrent_drops_release_once() {
  loom::model(|| {
    let (sbox, dropped) = new_payload();
    let other = sbox.clone();
    let handle = thread::spawn(move || drop(other));

    drop(sbox);
    handle.join().unwrap();

    assert_eq!(dropped.load(Ordering::Relaxed), 1);
  });
}

#[test]
fn clone_during_drop_releases_once() {
  loom::model(|| {
    let (sbox, dropped) = new_payload();
    let other = sbox.clone();
    let handle = thread::spawn(move || {
      let again = other.clone();
      drop(other);
      drop(again);
    });

    drop(sbox);
    handle.join().unwrap();

    assert_eq!(dropped.load(Ordering::Relaxed), 1);
  });
}

#[test]
fn writes_before_drop_are_visible_to_last_drop() {
  loom::model(|| {
    let (sbox, dropped) = new_payload();
    let other = sbox.clone();
    let handle = thread::spawn(move || {
      other.write();
      drop(other);
    });

    drop(sbox);
    handle.join().unwrap();

    assert_eq!(dropped.load(Ordering::Relaxed), 1);
  });
}

/*
 * The last dropper may only observe the count written by the intermediate
 * drop, so the writer's release must reach it through the `fetch_sub` chain.
 */
#[test]
fn writes_reach_last_drop_through_intermediate_drop() {
  loom::model(|| {
    let (sbox, dropped) = new_payload();
    let writer = {
      let other = sbox.clone();
      thread::spawn(move || {
        other.write();
        drop(other);
      })
    };
    let dropper = {
      let other = sbox.clone();
      thread::spawn(move || drop(other))
    };

    drop(sbox);
    writer.join().unwrap();
    dropper.join().unwrap();

    assert_eq!(dropped.load(Ordering::Relaxed), 1);
  });
}
