use std::{
  any::{Any, TypeId},
  collections::HashMap,
  marker::PhantomData,
  sync::{Arc, Weak},
  thread::{park, Builder, Thread},
};

use crossbeam::{queue::SegQueue, utils::Backoff};

use super::{ThreadSlot, UnwindSpawner};
use crate::{error, utils::SBox};

/**
 * Subscriber for events that may be observed by multiple consumers.
 *
 * Shared events are delivered as `Arc<E>`, allowing the same event object to be
 * seen by every registered subscriber. Use this for global notifications or
 * fan-out style events.
 */
pub trait SharedSubscription<E> {
  fn handle(&self, event: Arc<E>);
}
/**
 * Subscriber for events that have exactly one consumer.
 *
 * Owned events are moved into the subscriber. Use this for event payloads whose
 * ownership must be transferred to the handler.
 */
pub trait OwnedSubscription<E> {
  fn handle(&self, event: E);
}

type OwnedEvent = Box<dyn Any + Send + Sync>;
type SharedEvent = Arc<dyn Any + Send + Sync>;

trait OwnedEventAdapter {
  fn cast_event(&self, event: OwnedEvent) -> bool;
  fn drain_events(&self, events: &mut Vec<OwnedEvent>) {
    for event in events.drain(..) {
      self.cast_event(event);
    }
  }
}
trait SharedEventAdapter {
  fn cast_event(&self, event: SharedEvent) -> bool;
}
/*
 * The event bus does not own subscribers.
 *
 * Routes store `Weak` references so registering a subscriber does not extend
 * its lifetime. If the subscriber has been dropped, delivery fails naturally:
 * owned routes become tombstones and shared routes remove the dead adapter.
 */
struct AdapterImpl<E, S: ?Sized> {
  subscriber: Weak<S>,
  _event: PhantomData<E>,
}
impl<E, S> OwnedEventAdapter for AdapterImpl<E, S>
where
  E: Any + Send + Sync + 'static,
  S: OwnedSubscription<E> + ?Sized,
{
  fn cast_event(&self, event: OwnedEvent) -> bool {
    if let Some(sub) = self.subscriber.upgrade() {
      if let Ok(e) = event.downcast::<E>() {
        sub.handle(*e);
      }
      return true;
    }
    false
  }
}
impl<E, S> SharedEventAdapter for AdapterImpl<E, S>
where
  E: Any + Send + Sync + 'static,
  S: SharedSubscription<E> + ?Sized,
{
  fn cast_event(&self, event: SharedEvent) -> bool {
    if let Some(sub) = self.subscriber.upgrade() {
      if let Ok(e) = Arc::downcast(event) {
        sub.handle(e);
      }
      return true;
    }
    false
  }
}
impl<E, S: ?Sized> AdapterImpl<E, S> {
  fn new(subscriber: Weak<S>) -> Self {
    Self {
      subscriber,
      _event: PhantomData,
    }
  }
}
enum Route {
  /*
   * Exactly one owned subscriber is registered for this event type.
   *
   * Owned events are usually important hand-off messages, so the bus preserves
   * them during startup until the owning subscriber is registered.
   */
  Owned(Box<dyn OwnedEventAdapter>),
  /*
   * Zero or more shared subscribers are registered for this event type.
   *
   * Shared events are fan-out notifications and are delivered as `Arc`.
   */
  Shared(Vec<Box<dyn SharedEventAdapter>>),
  /*
   * Events published before their owned subscriber is registered.
   *
   * This exists to avoid tight initialization-order coupling between engine
   * components. Once the subscriber is registered, the queued events are drained
   * into it and the route becomes `Owned`.
   */
  Offline(Vec<OwnedEvent>),
  /*
   * The previous owned subscriber disappeared.
   *
   * After startup, reaching this state should normally only happen during engine
   * shutdown. Future events of this type are ignored as a defensive measure.
   */
  Tombstone,
}

/*
 * Registration conflicts are programmer errors.
 *
 * An owned event type must have exactly one consumer, and an event type cannot
 * be both owned and shared. Shared routes support fan-out, but the ownership
 * model is still exclusive: a type is either an owned hand-off event or a shared
 * notification event, never both.
 */
struct EventRouter {
  handlers: HashMap<TypeId, Route>,
}
impl EventRouter {
  fn new() -> Self {
    Self {
      handlers: HashMap::new(),
    }
  }

  fn register_owned(&mut self, id: TypeId, handler: Box<dyn OwnedEventAdapter>) -> bool {
    let Some(route) = self.handlers.get_mut(&id) else {
      self.handlers.insert(id, Route::Owned(handler));
      return true;
    };

    match route {
      Route::Offline(queue) => {
        handler.drain_events(queue);
        *route = Route::Owned(handler);
      }
      Route::Tombstone => *route = Route::Owned(handler),
      _ => return false,
    }
    true
  }
  fn register_shared(
    &mut self,
    id: TypeId,
    handler: Box<dyn SharedEventAdapter>,
  ) -> bool {
    let Some(route) = self.handlers.get_mut(&id) else {
      self.handlers.insert(id, Route::Shared(vec![handler]));
      return true;
    };

    match route {
      Route::Owned(_) => return false,
      Route::Shared(handlers) => handlers.push(handler),
      _ => *route = Route::Shared(vec![handler]),
    }
    true
  }

  fn route(&mut self, event: OwnedEvent) {
    let id = (*event).type_id();
    let Some(route) = self.handlers.get_mut(&id) else {
      self.handlers.insert(id, Route::Offline(vec![event]));
      return;
    };

    match route {
      Route::Owned(owned) => {
        if owned.cast_event(event) {
          return;
        };
        *route = Route::Tombstone;
      }
      Route::Shared(shared) => {
        let event: SharedEvent = Arc::from(event);
        shared.retain(|h| h.cast_event(event.clone()));
      }
      Route::Offline(queue) => {
        queue.push(event);
      }
      Route::Tombstone => {}
    };
  }
}

pub trait OwnedEventList<S: ?Sized> {
  fn bind(bus: &EventBus, subscriber: Weak<S>);
}
impl<S: ?Sized> OwnedEventList<S> for () {
  fn bind(_: &EventBus, _: Weak<S>) {}
}
impl<A, B, S> OwnedEventList<S> for (A, B)
where
  A: Send + Sync + 'static,
  B: OwnedEventList<S>,
  S: OwnedSubscription<A> + Send + Sync + ?Sized + 'static,
{
  fn bind(bus: &EventBus, subscriber: Weak<S>) {
    bus.bind_owned(subscriber.clone());
    B::bind(bus, subscriber)
  }
}
pub trait SharedEventList<S: ?Sized> {
  fn bind(bus: &EventBus, subscriber: Weak<S>);
}
impl<S: ?Sized> SharedEventList<S> for () {
  fn bind(_: &EventBus, _: Weak<S>) {}
}
impl<A, B, S> SharedEventList<S> for (A, B)
where
  A: Send + Sync + 'static,
  B: SharedEventList<S>,
  S: SharedSubscription<A> + ?Sized + Send + Sync + 'static,
{
  fn bind(bus: &EventBus, subscriber: Weak<S>) {
    bus.bind_shared(subscriber.clone());
    B::bind(bus, subscriber)
  }
}
/**
 * Compile-time list of owned events handled by a subscriber.
 *
 * The `binding_events!` macro expands event arrays into recursive tuple lists.
 * Calling `bind` walks that type list and registers a route from each event
 * type to the subscriber.
 */
pub trait EventBindings {
  type Owned: OwnedEventList<Self>;
  type Shared: SharedEventList<Self>;
}

#[macro_export]
macro_rules! binding_events {
  (@list) => { () };

  (@list $head:ty $(, $tail:ty)*) => {
    ($head, $crate::binding_events!(@list $($tail),*))
  };

  ($subscriber:ty {
    shared: [$($shared:ty),* $(,)?],
    owned: [$($owned:ty),* $(,)?] $(,)?
  }) => {
    $crate::binding_events!(@impl $subscriber, [$($shared),*], [$($owned),*]);
  };
  ($subscriber:ty {
    owned: [$($owned:ty),* $(,)?],
    shared: [$($shared:ty),* $(,)?] $(,)?
  }) => {
    $crate::binding_events!(@impl $subscriber, [$($shared),*], [$($owned),*]);
  };

  ($subscriber:ty {
    shared: [$($shared:ty),* $(,)?] $(,)?
  }) => {
    $crate::binding_events!(@impl $subscriber, [$($shared),*], []);
  };

  ($subscriber:ty {
    owned: [$($owned:ty),* $(,)?] $(,)?
  }) => {
    $crate::binding_events!(@impl $subscriber, [], [$($owned),*]);
  };

  (@impl $subscriber:ty, [$($shared:ty),*], [$($owned:ty),*]) => {
    impl $crate::background::EventBindings for $subscriber {
      type Shared = $crate::binding_events!(@list $($shared),*);
      type Owned = $crate::binding_events!(@list $($owned),*);
    }
  };
}
enum EventMsg {
  Publish(OwnedEvent),
  SubShared(TypeId, Box<dyn SharedEventAdapter + Send + Sync>),
  SubOwned(TypeId, Box<dyn OwnedEventAdapter + Send + Sync>),
  Terminate,
}

const fn handle_thread(queue: SBox<SegQueue<EventMsg>>) -> impl FnOnce() {
  move || {
    let mut map = EventRouter::new();
    let backoff = Backoff::new();

    loop {
      while !backoff.is_completed() {
        let Some(msg) = queue.pop() else {
          backoff.snooze();
          continue;
        };

        match msg {
          EventMsg::Publish(event) => map.route(event),
          EventMsg::SubOwned(id, handler) => {
            if !map.register_owned(id, handler) {
              error!("error to register {:?} as owned event subscriber", id);
              std::process::abort();
            };
          }
          EventMsg::SubShared(id, handler) => {
            if !map.register_shared(id, handler) {
              error!("error to register {:?} as owned event subscriber", id);
              std::process::abort();
            };
          }
          EventMsg::Terminate => return,
        }
        backoff.reset();
      }

      park();
      backoff.reset();
    }
  }
}

pub struct EventBus {
  queue: SBox<SegQueue<EventMsg>>,
  waker: Thread,
  slot: ThreadSlot,
}
impl EventBus {
  pub fn new() -> Self {
    let queue = SBox::new(SegQueue::new());
    let handle = Builder::new()
      .name("event bus".to_string())
      .stack_size(2 << 20)
      .spawn_unwind(handle_thread(queue.clone()));
    let waker = handle.thread().clone();
    Self {
      queue,
      waker,
      slot: ThreadSlot::new(handle),
    }
  }

  fn bind_shared<E, S>(&self, subscriber: Weak<S>)
  where
    E: Send + Sync + 'static,
    S: SharedSubscription<E> + Send + Sync + ?Sized + 'static,
  {
    let adapter = AdapterImpl::<E, S>::new(subscriber);
    self
      .queue
      .push(EventMsg::SubShared(TypeId::of::<E>(), Box::new(adapter)));
    self.waker.unpark();
  }
  fn bind_owned<E, S>(&self, subscriber: Weak<S>)
  where
    E: Send + Sync + 'static,
    S: OwnedSubscription<E> + Send + Sync + ?Sized + 'static,
  {
    let adapter = AdapterImpl::<E, S>::new(subscriber);
    self
      .queue
      .push(EventMsg::SubOwned(TypeId::of::<E>(), Box::new(adapter)));
    self.waker.unpark();
  }

  pub fn register<S>(&self, subscriber: &Arc<S>)
  where
    S: EventBindings + ?Sized,
  {
    let sub = Arc::downgrade(subscriber);
    S::Owned::bind(self, sub.clone());
    S::Shared::bind(self, sub);
  }

  pub fn publish<E: Any + Send + Sync>(&self, event: E) {
    self.queue.push(EventMsg::Publish(Box::new(event)));
    self.waker.unpark();
  }
  pub fn batch_publish<E, I>(&self, events: I)
  where
    E: Any + Send + Sync,
    I: Iterator<Item = E>,
  {
    for event in events {
      self.queue.push(EventMsg::Publish(Box::new(event)));
    }
    self.waker.unpark();
  }

  pub fn close(&self) {
    if let Some(handle) = self.slot.close() {
      self.queue.push(EventMsg::Terminate);
      self.waker.unpark();
      handle.join().unwrap();
    }
  }
}

#[cfg(test)]
#[path = "tests/event_bus.rs"]
mod tests;
