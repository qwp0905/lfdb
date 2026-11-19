use loom::{sync::Arc, thread};

use super::*;

#[test]
fn fulfilled_value_is_received() {
  loom::model(|| {
    let probe = Arc::new(());
    let (tx, rx) = create_task();
    let sender = {
      let probe = probe.clone();
      thread::spawn(move || tx.fulfill(probe))
    };

    let received = rx.wait().unwrap();
    sender.join().unwrap();

    assert!(Arc::ptr_eq(&received, &probe));
    drop(received);
    assert_eq!(Arc::strong_count(&probe), 1);
  });
}

#[test]
fn dropped_fulfiller_disconnects_waiter() {
  loom::model(|| {
    let (tx, rx) = create_task::<()>();
    let sender = thread::spawn(move || drop(tx));

    assert!(rx.wait().is_err());
    sender.join().unwrap();
  });
}

#[test]
fn value_is_dropped_once_when_receiver_drops() {
  loom::model(|| {
    let probe = Arc::new(());
    let (tx, rx) = create_task();
    let sender = {
      let probe = probe.clone();
      thread::spawn(move || tx.fulfill(probe))
    };

    drop(rx);
    sender.join().unwrap();

    assert_eq!(Arc::strong_count(&probe), 1);
  });
}

#[test]
fn receiver_moved_after_park_is_woken() {
  loom::model(|| {
    let (tx, rx) = create_task::<usize>();
    let rx = thread::spawn(move || {
      assert!(rx.try_park());
      rx
    })
    .join()
    .unwrap();

    let sender = thread::spawn(move || tx.fulfill(7));
    let waiter = thread::spawn(move || rx.wait().unwrap());

    assert_eq!(waiter.join().unwrap(), 7);
    sender.join().unwrap();
  });
}
