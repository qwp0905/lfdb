use loom::{
  cell::UnsafeCell,
  sync::{atomic::AtomicUsize, Arc},
  thread,
};

use super::*;

fn spawn_sender<T: Send + 'static>(
  behavior: &Arc<OneshotBehavior<T>>,
  value: Option<T>,
) -> thread::JoinHandle<()> {
  let behavior = behavior.clone();
  thread::spawn(move || {
    if let Some(value) = value {
      behavior.fulfill_and_wake(value);
    }
    behavior.drop_sender();
  })
}

#[test]
fn fulfilled_value_is_received() {
  loom::model(|| {
    let probe = Arc::new(());
    let behavior = Arc::new(OneshotBehavior::new());
    let sender = spawn_sender(&behavior, Some(probe.clone()));

    let received = behavior.wait().unwrap();
    behavior.drop_receiver();
    sender.join().unwrap();

    assert!(Arc::ptr_eq(&received, &probe));
    drop(received);
    assert_eq!(Arc::strong_count(&probe), 1);
  });
}

#[test]
fn dropped_sender_disconnects_waiter() {
  loom::model(|| {
    let behavior = Arc::new(OneshotBehavior::<()>::new());
    let sender = spawn_sender(&behavior, None);

    assert!(behavior.wait().is_err());
    behavior.drop_receiver();
    sender.join().unwrap();
  });
}

#[test]
fn value_is_dropped_once_when_receiver_drops() {
  loom::model(|| {
    let probe = Arc::new(());
    let behavior = Arc::new(OneshotBehavior::new());
    let sender = spawn_sender(&behavior, Some(probe.clone()));

    behavior.drop_receiver();
    sender.join().unwrap();

    assert_eq!(Arc::strong_count(&probe), 1);
  });
}

#[test]
fn callback_or_waiter_receives_value_once() {
  loom::model(|| {
    let received = Arc::new(AtomicUsize::new(0));
    let behavior = Arc::new(OneshotBehavior::new());
    let sender = spawn_sender(&behavior, Some(1));

    // The callback may run on the sender thread, so what it captures has to be
    // published through the slot.
    let callback = {
      let received = received.clone();
      let captured = UnsafeCell::new(0);
      captured.with_mut(|c| unsafe { *c = 1 });
      Callback::new(move |v: &usize| {
        assert_eq!(captured.with(|c| unsafe { *c }), 1);
        received.fetch_add(*v, Ordering::Relaxed);
      })
    };
    if let Err(callback) = behavior.add_callback(callback) {
      callback.call(&behavior.wait().unwrap());
    }
    behavior.drop_receiver();
    sender.join().unwrap();

    assert_eq!(received.load(Ordering::Relaxed), 1);
  });
}
