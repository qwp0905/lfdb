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

  let bus = Arc::new(EventBus::new());
  let mut router = EventRouter::new(bus.clone());
  bus.register(&h);

  let e1c = 10;
  let e2c = 5;

  for _ in 0..e1c {
    bus.publish(E1);
  }
  for _ in 0..e2c {
    bus.publish(E2);
  }
  unsafe { router.run_tick() };

  assert_eq!(E1_C.load(Ordering::Acquire), e1c);
  assert_eq!(E2_C.load(Ordering::Acquire), e2c);
}

#[test]
fn test_shared() {
  static E1_C: AtomicUsize = AtomicUsize::new(0);
  static E2_C: AtomicUsize = AtomicUsize::new(0);

  struct EV;
  struct S1;
  impl SharedSubscription<EV> for S1 {
    fn handle(&self, _: Arc<EV>) {
      E1_C.fetch_add(1, Ordering::Relaxed);
    }
  }
  binding_events!(S1 { shared: [EV] });

  struct S2;
  impl SharedSubscription<EV> for S2 {
    fn handle(&self, _: Arc<EV>) {
      E2_C.fetch_add(1, Ordering::Relaxed);
    }
  }
  binding_events!(S2 { shared: [EV] });

  let s1 = Arc::new(S1);
  let s2 = Arc::new(S2);

  let ev = Arc::new(EventBus::new());
  let mut router = EventRouter::new(ev.clone());
  ev.register(&s1);
  ev.register(&s2);

  ev.publish(EV);
  ev.publish(EV);
  ev.publish(EV);
  unsafe { router.run_tick() };

  assert_eq!(E1_C.load(Ordering::Relaxed), 3);
  assert_eq!(E2_C.load(Ordering::Relaxed), 3);
}

#[test]
fn test_owned_event_before_register_is_delivered() {
  static RECEIVED: AtomicUsize = AtomicUsize::new(0);

  struct EV;
  struct S;
  impl OwnedSubscription<EV> for S {
    fn handle(&self, _: EV) {
      RECEIVED.fetch_add(1, Ordering::Relaxed);
    }
  }
  binding_events!(S { owned: [EV] });

  let bus = Arc::new(EventBus::new());
  let mut router = EventRouter::new(bus.clone());

  bus.publish(EV);
  unsafe { router.run_tick() };
  assert_eq!(RECEIVED.load(Ordering::Relaxed), 0);

  let s = Arc::new(S);
  bus.register(&s);
  unsafe { router.run_tick() };
  assert_eq!(RECEIVED.load(Ordering::Relaxed), 1);
}
