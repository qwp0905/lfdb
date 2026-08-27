use super::{EventBindings, Oneshot, OwnedSubscription};

/**
 * Stop the runtime and join its worker thread(s).
 *
 * `close` is the synchronization boundary for background runtimes. After it
 * is called, the runtime stops accepting requests and the caller waits for
 * the underlying thread(s) to terminate. Implementations use this point to
 * join the worker thread(s), which also makes background panics observable by
 * the caller.
 */
pub trait Close: Send + Sync {
  fn close(&self);
}

/**
 * Submit a command and return a completion handle.
 *
 * `execute` is used when the caller needs a response from the background
 * runtime. The work item is wrapped in a `Context::Work` together with a
 * oneshot fulfiller, and the returned `Oneshot` can be waited on by the
 * caller.
 */
pub trait Execute<T, R>: Close {
  fn execute(&self, value: T) -> Oneshot<R>;
}
/**
 * Submit a fire-and-forget event.
 *
 * `dispatch` is used when the caller only needs to notify the background
 * runtime and does not need a response. In DDD terms, `execute` behaves like a
 * command with a reply, while `dispatch` behaves like an event.
 */
pub trait Dispatch<T>: Close {
  fn dispatch(&self, value: T);
}

/**
 * Allow a background runtime to be registered directly as an event subscriber.
 *
 * Event bus deliveries do not expect a response, so owned events are forwarded
 * through `dispatch`. This implementation is the bridge used by the
 * `binding_events!` macro: a background thread can act as the runtime for an
 * event subscription without exposing its concrete runtime type.
 */
impl<T> OwnedSubscription<T> for dyn Dispatch<T> {
  fn handle(&self, event: T) {
    self.dispatch(event);
  }
}
impl<T> EventBindings for dyn Dispatch<T>
where
  T: Send + Sync + 'static,
{
  type Owned = (T, ());

  type Shared = ();
}
