use super::{oneshot, Context, EventBindings, Oneshot, OwnedSubscription};

/**
 * Common interface for all background runtimes used by the engine.
 *
 * A BackgroundThread is intentionally broader than a plain worker queue: it is
 * the public handle to a background execution runtime. Implementations may run
 * as a single worker, a shared worker pool, an eager worker, or an interval
 * task, but they all expose the same way to submit work, dispatch fire-and-
 * forget messages, and shut the runtime down.
 *
 * This trait hides the scheduling model behind each background thread type so
 * callers do not need to know how the work is actually executed.
 */
pub trait BackgroundThread<T, R = ()>: Send + Sync {
  /**
   * Submit a raw execution context to the runtime.
   *
   * This is the low-level entry point implemented by each runtime type. The
   * trait-level helper methods decide which Context variant represents the
   * caller's request, while each concrete runtime decides how that context
   * should be queued, scheduled, or handled.
   */
  fn register(&self, ctx: Context<T, R>);
  /**
   * Stop the runtime and join its worker thread(s).
   *
   * `close` is the synchronization boundary for background runtimes. After it
   * is called, the runtime stops accepting requests and the caller waits for
   * the underlying thread(s) to terminate. Implementations use this point to
   * join the worker thread(s), which also makes background panics observable by
   * the caller.
   */
  fn close(&self);

  /**
   * Submit a command and return a completion handle.
   *
   * `execute` is used when the caller needs a response from the background
   * runtime. The work item is wrapped in a `Context::Work` together with a
   * oneshot fulfiller, and the returned `Oneshot` can be waited on by the
   * caller.
   */
  #[inline]
  fn execute(&self, v: T) -> Oneshot<R> {
    let (done_r, done_t) = oneshot();
    self.register(Context::Work(v, done_t));
    done_r
  }

  /**
   * Submit a fire-and-forget event.
   *
   * `dispatch` is used when the caller only needs to notify the background
   * runtime and does not need a response. In DDD terms, `execute` behaves like a
   * command with a reply, while `dispatch` behaves like an event.
   */
  fn dispatch(&self, v: T) {
    self.register(Context::Dispatch(v));
  }
}

/**
 * Allow a background runtime to be registered directly as an event subscriber.
 *
 * Event bus deliveries do not expect a response, so owned events are forwarded
 * through `dispatch`. This implementation is the bridge used by the
 * `binding_events!` macro: a background thread can act as the runtime for an
 * event subscription without exposing its concrete runtime type.
 */
impl<T, R> OwnedSubscription<T> for dyn BackgroundThread<T, R> {
  fn handle(&self, event: T) {
    self.dispatch(event);
  }
}
impl<T, R> EventBindings for dyn BackgroundThread<T, R>
where
  T: Send + Sync + 'static,
  R: 'static,
{
  type Owned = (T, ());

  type Shared = ();
}
