use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

#[test]
fn test_simple() {
  static E1_C: AtomicUsize = AtomicUsize::new(0);
  static E2_C: AtomicUsize = AtomicUsize::new(0);

  struct E1;
  struct E2;
  struct H;

  impl OwnedSubscription<E1> for H {
    fn handle(&self, _: E1) {
      E1_C.fetch_add(1, Ordering::Relaxed);
    }
  }
  impl SharedSubscription<E2> for H {
    fn handle(&self, _: Arc<E2>) {
      E2_C.fetch_add(1, Ordering::Relaxed);
    }
  }

  binding_events!(H {
    shared: [E2],
    owned: [E1],
  });

  let h = Arc::new(H);

  let bus = EventBus::new();
  bus.register(&h);

  let e1c = 10;
  let e2c = 5;

  for _ in 0..e1c {
    bus.publish(E1);
  }
  for _ in 0..e2c {
    bus.publish(E2);
  }

  bus.close();

  assert_eq!(E1_C.load(Ordering::Acquire), e1c);
  assert_eq!(E2_C.load(Ordering::Acquire), e2c);
}
