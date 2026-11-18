use loom::{sync::Arc, thread};

use super::*;

fn spawn_sender<T: Send + 'static>(
  oneshot: &Arc<Oneshot<T>>,
  value: Option<T>,
) -> thread::JoinHandle<()> {
  let oneshot = oneshot.clone();
  thread::spawn(move || {
    if let Some(value) = value {
      oneshot.fulfill_and_wake(value);
    }
    oneshot.drop_sender();
  })
}

#[test]
fn fulfilled_value_is_received() {
  loom::model(|| {
    let probe = Arc::new(());
    let oneshot = Arc::new(Oneshot::new());
    let sender = spawn_sender(&oneshot, Some(probe.clone()));

    let received = oneshot.wait().unwrap();
    oneshot.drop_receiver();
    sender.join().unwrap();

    assert!(Arc::ptr_eq(&received, &probe));
    drop(received);
    assert_eq!(Arc::strong_count(&probe), 1);
  });
}

#[test]
fn dropped_sender_disconnects_waiter() {
  loom::model(|| {
    let oneshot = Arc::new(Oneshot::<()>::new());
    let sender = spawn_sender(&oneshot, None);

    assert!(oneshot.wait().is_err());
    oneshot.drop_receiver();
    sender.join().unwrap();
  });
}

#[test]
fn value_is_dropped_once_when_receiver_drops() {
  loom::model(|| {
    let probe = Arc::new(());
    let oneshot = Arc::new(Oneshot::new());
    let sender = spawn_sender(&oneshot, Some(probe.clone()));

    oneshot.drop_receiver();
    sender.join().unwrap();

    assert_eq!(Arc::strong_count(&probe), 1);
  });
}
