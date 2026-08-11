use super::{
  oneshot, BufferingThread, Context, EventBindings, IntervalWorkThread, Oneshot,
  OwnedSubscription, PreloadThread, StealingWorkThread,
};

pub enum ThreadTypes<T, R> {
  Stealing(StealingWorkThread<T, R>),
  Preload(PreloadThread<T, R>),
  Interval(IntervalWorkThread<T, R>),
  Buffering(BufferingThread<T, R>),
}
/**
 * Common interface for all background runtimes used by the engine.
 *
 * A BackgroundThread is intentionally broader than a plain worker queue: it is
 * the public handle to a background execution runtime. Implementations may run
 * as a single worker, a stealing worker pool, an eager worker, or an interval
 * task, but they all expose the same way to submit work, dispatch fire-and-
 * forget messages, and shut the runtime down.
 */
pub struct BackgroundThread<T, R = ()>(ThreadTypes<T, R>);
impl<T, R> BackgroundThread<T, R> {
  pub const fn new(types: ThreadTypes<T, R>) -> Self {
    Self(types)
  }

  /**
   * Submit a raw execution context to the runtime.
   *
   * This is the low-level entry point implemented by each runtime type. The
   * trait-level helper methods decide which Context variant represents the
   * caller's request, while each concrete runtime decides how that context
   * should be queued, scheduled, or handled.
   */
  fn register(&self, ctx: Context<T, R>) {
    match &self.0 {
      ThreadTypes::Stealing(t) => t.register(ctx),
      ThreadTypes::Preload(t) => t.register(ctx),
      ThreadTypes::Interval(t) => t.register(ctx),
      ThreadTypes::Buffering(t) => t.register(ctx),
    };
  }

  /**
   * Submit a command and return a completion handle.
   *
   * `execute` is used when the caller needs a response from the background
   * runtime. The work item is wrapped in a `Context::Work` together with a
   * oneshot fulfiller, and the returned `Oneshot` can be waited on by the
   * caller.
   */
  pub fn execute(&self, value: T) -> Oneshot<R> {
    let (done_r, done_t) = oneshot();
    let ctx = Context::Work(value, done_t);
    match &self.0 {
      ThreadTypes::Stealing(t) => t.register(ctx),
      ThreadTypes::Preload(t) => t.register(ctx),
      ThreadTypes::Interval(t) => t.register(ctx),
      ThreadTypes::Buffering(t) => t.register(ctx),
    };
    done_r
  }

  /**
   * Submit a fire-and-forget event.
   *
   * `dispatch` is used when the caller only needs to notify the background
   * runtime and does not need a response. In DDD terms, `execute` behaves like a
   * command with a reply, while `dispatch` behaves like an event.
   */
  pub fn dispatch(&self, value: T) {
    self.register(Context::Dispatch(value));
  }

  /**
   * Stop the runtime and join its worker thread(s).
   *
   * `close` is the synchronization boundary for background runtimes. After it
   * is called, the runtime stops accepting requests and the caller waits for
   * the underlying thread(s) to terminate. Implementations use this point to
   * join the worker thread(s), which also makes background panics observable by
   * the caller.
   */
  pub fn close(&self) {
    match &self.0 {
      ThreadTypes::Stealing(t) => t.close(),
      ThreadTypes::Preload(t) => t.close(),
      ThreadTypes::Interval(t) => t.close(),
      ThreadTypes::Buffering(t) => t.close(),
    };
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
impl<T, R> OwnedSubscription<T> for BackgroundThread<T, R> {
  fn handle(&self, event: T) {
    self.dispatch(event);
  }
}
impl<T, R> EventBindings for BackgroundThread<T, R>
where
  T: Send + Sync + 'static,
  R: Send + 'static,
{
  type Owned = (T, ());

  type Shared = ();
}
